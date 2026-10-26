import process from "process"
import { SupabaseClient, createClient } from "@supabase/supabase-js"

import { createRxDatabase, RxCollection, RxDatabase, addRxPlugin } from "rxdb"
import { RxDBDevModePlugin } from "rxdb/plugins/dev-mode"
import { RxReplicationState } from "rxdb/plugins/replication"
import { getRxStorageMemory } from "rxdb/plugins/storage-memory"
import { wrappedValidateAjvStorage } from "rxdb/plugins/validate-ajv"
import { lastValueFrom, take } from "rxjs"
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest"
import {
  SupabaseReplication,
  SupabaseReplicationCheckpoint,
  SupabaseReplicationOptions,
} from "../supabase-replication.js"
import { Human, HUMAN_SCHEMA, MODIFIED_FIELD } from "../test-utils/test-types.js"
import { withReplication } from "../test-utils/test-utils.js"

type HumanRow = Human & { is_deleted: boolean }

/**
 * Integration test running against an actual Supabase instance.
 *
 * This test will only run if the INTEGRATION_TEST, TEST_SUPABASE_URL and TEST_SUPABASE_API_KEY
 * environment variables are present. It requires a "humans" table to be created, see humans.sql
 * for the table structure.
 */
describe.skipIf(!process.env.INTEGRATION_TEST)(
  "replicateSupabase with actual SupabaseClient",
  () => {
    let supabase: SupabaseClient
    let db: RxDatabase
    let collection: RxCollection<Human>

    beforeAll(() => {
      supabase = createClient(process.env.TEST_SUPABASE_URL!, process.env.TEST_SUPABASE_API_KEY!, {
        auth: { persistSession: false },
      })
      addRxPlugin(RxDBDevModePlugin)
    })

    beforeEach(async () => {
      // Empty the supabase table.
      const { error } = await supabase.from("humans").delete().neq("id", "-1")
      if (error) throw error

      // Create an in-memory RxDB database.
      db = await createRxDatabase({
        name: "test",
        storage: wrappedValidateAjvStorage({ storage: getRxStorageMemory() }),
      })
      collection = (
        await db.addCollections({
          humans: { schema: HUMAN_SCHEMA },
        })
      ).humans

      // Start with Alice :)
      await replication({}, async () => {
        await collection.insert({ id: "1", name: "Alice", age: null })
      })
      expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: null }])
      expect(await supabaseContents()).toEqual([
        { id: "1", name: "Alice", age: null, is_deleted: false },
      ])
    })

    describe("on client-side insertion", () => {
      it("inserts into supabase", async () => {
        await replication({}, async () => {
          await collection.insert({ id: "2", name: "Bob", age: null })
        })

        expect(await supabaseContents()).toEqual([
          { id: "1", name: "Alice", age: null, is_deleted: false },
          { id: "2", name: "Bob", age: null, is_deleted: false },
        ])
      })
    })

    describe("on client-side update", () => {
      it("updates supabase", async () => {
        await replication({}, async () => {
          const doc = await collection.findOne("1").exec()
          await doc!.patch({ age: 42 })
        })

        expect(await supabaseContents()).toEqual([
          { id: "1", name: "Alice", age: 42, is_deleted: false },
        ])
      })

      describe("with postgREST special characters", () => {
        it("updates supabase", async () => {
          await replication({}, async () => {
            await collection.insert({ id: "special-,.()-id", name: 'Robert "Bob" Doe', age: null })
          })
          await replication({}, async () => {
            const doc = await collection.findOne("special-,.()-id").exec()
            await doc!.patch({ age: 42 })
          })

          expect(await rxdbContents()).toEqual([
            { id: "1", name: "Alice", age: null },
            { id: "special-,.()-id", name: 'Robert "Bob" Doe', age: 42 },
          ])
          expect(await supabaseContents()).toEqual([
            { id: "1", name: "Alice", age: null, is_deleted: false },
            { id: "special-,.()-id", name: 'Robert "Bob" Doe', age: 42, is_deleted: false },
          ])
        })
      })
    })

    describe("on client-side delete", () => {
      it("marks the row as deleted in supabase", async () => {
        await replication({}, async () => {
          const doc = await collection.findOne("1").exec()
          await doc!.remove()
        })

        expect(await rxdbContents()).toEqual([])
        expect(await supabaseContents()).toEqual([
          { id: "1", name: "Alice", age: null, is_deleted: true },
        ])
      })
    })

    describe("when supabase changed while offline", () => {
      it("pulls new rows", async () => {
        await supabase.from("humans").insert({ id: "2", name: "Bob", age: 42 })
        await replication()

        expect(await rxdbContents()).toEqual([
          { id: "1", name: "Alice", age: null },
          { id: "2", name: "Bob", age: 42 },
        ])
      })

      it("pulls rows in multiple batches", async () => {
        // Insert rows separately so they get distinct modified timestamps. The pull query only
        // fetches rows strictly newer than the checkpoint, without primary key tie-breaking.
        await supabase.from("humans").insert({ id: "2", name: "Human 2", age: null })
        await supabase.from("humans").insert({ id: "3", name: "Human 3", age: null })
        await replication({ pull: { batchSize: 1 } })

        expect(await rxdbContents()).toHaveLength(3)
      })

      it("pulls updated rows", async () => {
        await supabase.from("humans").update({ age: 42 }).eq("id", "1")
        await replication()

        expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: 42 }])
      })

      it("does not pull rows marked as deleted", async () => {
        await supabase.from("humans").insert({ id: "2", name: "Bob", age: 42, is_deleted: true })
        await replication()

        expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: null }])
      })

      it("ignores actually deleted rows", async () => {
        await supabase.from("humans").delete().eq("id", "1")
        await replication()

        expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: null }])
      })
    })

    describe("when supabase changed while online", () => {
      describe("without realtime replication", () => {
        it("does not pull new rows", async () => {
          await replication({}, async () => {
            await supabase.from("humans").insert({ id: "2", name: "Bob", age: 42 })
            await new Promise((resolve) => setTimeout(() => resolve(true), 1000)) // Wait for some time
          })

          expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: null }])
        })
      })

      describe("with realtime replication", () => {
        it("pulls new rows", async () => {
          await replication({ pull: { realtimePostgresChanges: true } }, async (replication) => {
            await supabase.from("humans").insert({ id: "2", name: "Bob", age: 42 })
            await lastValueFrom(replication.remoteEvents$.pipe(take(1))) // Wait for remote event
          })

          expect(await rxdbContents()).toEqual([
            { id: "1", name: "Alice", age: null },
            { id: "2", name: "Bob", age: 42 },
          ])
        })

        it("pulls updated rows", async () => {
          await replication({ pull: { realtimePostgresChanges: true } }, async (replication) => {
            await supabase.from("humans").update({ age: 42 }).eq("id", "1")
            await lastValueFrom(replication.remoteEvents$.pipe(take(1))) // Wait for remote event
          })

          expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: 42 }])
        })

        it("ignores actually deleted rows", async () => {
          await replication({ pull: { realtimePostgresChanges: true } }, async () => {
            await supabase.from("humans").delete().eq("id", "1")
            await new Promise((resolve) => setTimeout(() => resolve(true), 1000)) // Wait for some time
          })

          expect(await rxdbContents()).toEqual([{ id: "1", name: "Alice", age: null }])
        })
      })
    })

    const replication = (
      options: Partial<SupabaseReplicationOptions<Human>> = {},
      callback: (
        state: RxReplicationState<Human, SupabaseReplicationCheckpoint>,
      ) => Promise<void> = async () => {},
      expectErrors = false,
    ): Promise<Error[]> => {
      return withReplication(() => startReplication(options), callback, expectErrors)
    }

    const startReplication = (
      options: Partial<SupabaseReplicationOptions<Human>> = {},
    ): SupabaseReplication<Human> => {
      return new SupabaseReplication({
        replicationIdentifier: "test",
        supabaseClient: supabase,
        collection,
        deletedField: "is_deleted",
        push: {},
        ...options,
        pull: {
          realtimePostgresChanges: false,
          lastModifiedField: MODIFIED_FIELD,
          ...options.pull,
        },
      })
    }

    const supabaseContents = async (): Promise<HumanRow[]> => {
      const { data, error } = await supabase.from("humans").select().order("id")
      if (error) throw error
      data.forEach((human) => delete human[MODIFIED_FIELD])
      return data as HumanRow[]
    }

    // Strips the modified timestamp, which is only present on rows pulled from supabase.
    const rxdbContents = async (): Promise<Human[]> => {
      const results = await collection.find().exec()
      return results.map((doc) => {
        const { modified_at, ...human } = doc.toJSON()
        return human
      })
    }

    afterEach(async () => {
      await db.remove()
    })
  },
)
