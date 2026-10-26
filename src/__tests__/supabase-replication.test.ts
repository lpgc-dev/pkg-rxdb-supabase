import { RealtimePostgresChangesPayload } from "@supabase/realtime-js"
import {
  addRxPlugin,
  createRxDatabase,
  RxCollection,
  RxDatabase,
  RxReplicationWriteToMasterRow,
} from "rxdb"
import { RxDBDevModePlugin } from "rxdb/plugins/dev-mode"
import { RxReplicationState } from "rxdb/plugins/replication"
import { getRxStorageMemory } from "rxdb/plugins/storage-memory"
import { wrappedValidateAjvStorage } from "rxdb/plugins/validate-ajv"
import { firstValueFrom } from "rxjs"
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest"
import {
  SupabaseReplication,
  SupabaseReplicationCheckpoint,
  SupabaseReplicationOptions,
} from "../supabase-replication.js"
import { SupabaseBackendMock } from "../test-utils/supabase-backend-mock.js"
import { Human, HumanRow, HUMAN_SCHEMA, MODIFIED_FIELD } from "../test-utils/test-types.js"
import { withReplication, resolveConflictWithName } from "../test-utils/test-utils.js"

describe.skipIf(process.env.INTEGRATION_TEST)("replicateSupabase", () => {
  let supabaseMock: SupabaseBackendMock
  let db: RxDatabase
  let collection: RxCollection<Human>

  beforeAll(() => {
    addRxPlugin(RxDBDevModePlugin)
  })

  beforeEach(async () => {
    // Create an in-memory RxDB database.
    db = await createRxDatabase({
      name: "test",
      storage: wrappedValidateAjvStorage({ storage: getRxStorageMemory() }),
      ignoreDuplicate: true,
    })
    collection = (
      await db.addCollections({
        humans: { schema: HUMAN_SCHEMA },
      })
    ).humans

    // Supabase client with mocked HTTP.
    supabaseMock = new SupabaseBackendMock()
  })

  describe("initial pull", () => {
    describe("without initial checkpoint", () => {
      it("pulls all rows from supabase", async () => {
        expectPull().thenReturn(createHumans(1))
        await replication()

        expect(await rxdbContents()).toEqual([
          { id: "1", name: "Human 1", age: 11, modified_at: "2023-1" },
        ])
      })
    })

    describe("with previous checkpoint", () => {
      it("pulls only rows modified after the checkpoint", async () => {
        const checkpoint: SupabaseReplicationCheckpoint = {
          modified: "timestamp",
          primaryKeyValue: "pkv",
        }
        expectPull({ modifiedAfter: "timestamp" }).thenReturn(createHumans(1))
        await replication({ pull: { initialCheckpoint: checkpoint } })

        expect(await rxdbContents()).toEqual([
          { id: "1", name: "Human 1", age: 11, modified_at: "2023-1" },
        ])
      })
    })

    describe("with zero rows", () => {
      it("pulls no rows", async () => {
        expectPull().thenReturn([])
        await replication()

        expect(await rxdbContents()).toEqual([])
      })
    })

    describe("with many rows", () => {
      it("pulls in batches", async () => {
        const BATCH_SIZE = 13
        const humans = createHumans(BATCH_SIZE * 2 + 3)
        const afterHuman = (index: number) => ({
          limit: BATCH_SIZE,
          modifiedAfter: createHuman(index).modified_at,
        })

        // Expect three queries
        expectPull({ limit: BATCH_SIZE }).thenReturn(humans.slice(0, BATCH_SIZE))
        expectPull(afterHuman(BATCH_SIZE)).thenReturn(humans.slice(BATCH_SIZE, BATCH_SIZE * 2))
        expectPull(afterHuman(BATCH_SIZE * 2)).thenReturn(humans.slice(BATCH_SIZE * 2))

        await replication({ pull: { batchSize: BATCH_SIZE } })

        expect(await rxdbContents()).toHaveLength(humans.length)
      })
    })

    describe("with query failing", () => {
      it("retries automatically", async () => {
        expectPull().thenFail()
        expectPull().thenReturn(createHumans(1))

        const errors = await replication({ retryTime: 10 }, async () => {}, true)
        expect(errors).toHaveLength(1)
        expect(await rxdbContents()).toEqual([
          { id: "1", name: "Human 1", age: 11, modified_at: "2023-1" },
        ])
      })
    })

    describe("with fetchDeleted enabled", () => {
      it("does not filter out deleted rows", async () => {
        expectPull({ fetchDeleted: true }).thenReturn([])
        await replication({ pull: { fetchOptions: { fetchDeleted: true } } })
      })
    })

    describe("with columns not in the collection schema", () => {
      it("drops unknown and underscore-prefixed columns", async () => {
        expectPull().thenReturn([
          {
            id: "1",
            name: "Alice",
            age: null,
            modified_at: "2023-1",
            _deleted: false,
            is_deleted: false,
            not_in_schema: "dropped",
          },
        ])
        await replication()

        expect(await rxdbContents()).toEqual([
          { id: "1", name: "Alice", age: null, modified_at: "2023-1" },
        ])
      })
    })

    describe("with lastModifiedField not in the collection schema", () => {
      it("emits an error", async () => {
        expectPull({ modifiedField: "_modified" }).thenReturn([
          { id: "1", name: "Alice", age: null, _deleted: false, _modified: "2023-1" },
        ])
        const state = startReplication({ pull: { lastModifiedField: "_modified" } })
        const error: any = await firstValueFrom(state.error$)
        await state.cancel()

        expect(error.parameters.errors[0].message).toEqual(
          "lastModifiedField : '_modified' not found in collection schema",
        )
      })
    })

    describe("with queryParams", () => {
      it.each([
        { opType: undefined, value: "Alice", params: [["name", "eq.Alice"]] },
        { opType: "is", value: true, params: [["name", "is.true"]] },
        { opType: "cs", value: "Alice", params: [["name", 'cs.{"Alice"}']] },
        { opType: "json_cs", value: { a: 1 }, params: [["name", 'cs.{"a":1}']] },
        { opType: "or", value: ["Alice", "Bob"], params: [["name", "in.(Alice,Bob)"]] },
        { opType: "or", value: "Alice", params: [["name", "eq.Alice"]] },
        { opType: "not_eq", value: "Alice", params: [["name", "neq.Alice"]] },
        { opType: "gt", value: 5, params: [["name", "gt.5"]] },
        { opType: "lt", value: 5, params: [["name", "lt.5"]] },
        {
          opType: "between",
          value: { from: 1, to: 5 },
          params: [
            ["name", "gte.1"],
            ["name", "lte.5"],
          ],
        },
        { opType: "eq", value: null, params: [] },
        { opType: "eq", value: undefined, params: [] },
      ])("applies opType $opType with value $value", async ({ opType, value, params }) => {
        expectPull({ filters: params as [string, string][] }).thenReturn([])
        await replication({
          pull: { fetchOptions: { queryParams: [{ field: "name", value, opType }] } },
        })
      })

      it("applies multiple params in order", async () => {
        expectPull({
          filters: [
            ["name", "eq.Alice"],
            ["age", "gt.18"],
          ],
        }).thenReturn([])
        await replication({
          pull: {
            fetchOptions: {
              queryParams: [
                { field: "name", value: "Alice" },
                { field: "age", value: 18, opType: "gt" },
              ],
            },
          },
        })
      })
    })
  })

  describe("with client-side insertion", () => {
    describe("with single insertion", () => {
      it("inserts row to supabase", async () => {
        await collection.insert({ id: "1", name: "Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenReturn()

        await replication()
      })
    })

    describe("with multiple insertions", () => {
      it("triggers multiple INSERT calls", async () => {
        await collection.insert({ id: "1", name: "Alice", age: null })
        await collection.insert({ id: "2", name: "Bob", age: 42 })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenReturn()
        expectInsert('{"id":"2","name":"Bob","age":42,"_deleted":false}').thenReturn()

        await replication()
      })
    })

    describe("with custom deleted field", () => {
      it("uses specified field", async () => {
        await collection.insert({ id: "1", name: "Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"removed":false}').thenReturn()

        await replication({ deletedField: "removed" })
      })
    })

    describe("with network error", () => {
      it("automatically retries", async () => {
        await collection.insert({ id: "1", name: "Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenFail()
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenReturn()

        const errors = await replication({ retryTime: 10 }, async () => {}, true)
        expect(errors).toHaveLength(1)
      })
    })

    describe("with postgres error", () => {
      it("automatically retries", async () => {
        await collection.insert({ id: "1", name: "Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenReturnError(
          "53000",
          503,
        )
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenReturn()

        const errors = await replication({ retryTime: 10 }, async () => {}, true)
        expect(errors).toHaveLength(1)
      })
    })

    describe("with duplicate key error", () => {
      it("fetches current state, invokes conflict handler and updates by id", async () => {
        collection.conflictHandler = resolveConflictWithName("Resolved Alice")
        await collection.insert({ id: "1", name: "Local Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Local Alice","age":null,"_deleted":false}').thenReturnError(
          "23505",
        )
        // Should fetch current state on duplicate key error...
        expectSelectById("1").thenReturn([
          { id: "1", name: "Remote Alice", age: 42, _deleted: false, modified_at: "mod" },
        ])
        // ...and push the result of the conflict handler.
        expectUpdateById(
          "1",
          '{"id":"1","name":"Resolved Alice","age":null,"_deleted":false}',
        ).thenReturn()

        await replication()

        expect(await rxdbContents()).toEqual([{ id: "1", name: "Resolved Alice", age: null }])
      })
    })
  })

  describe("with client-side update", () => {
    beforeEach(async () => {
      await collection.insert({ id: "1", name: 'Robert "Bob" Simpson', age: null })
      expectPull().thenReturn([])
      expectInsert(
        '{"id":"1","name":"Robert \\"Bob\\" Simpson","age":null,"_deleted":false}',
      ).thenReturn()
    })

    describe("with default updateHandler", () => {
      it("performs UPDATE filtered by id only", async () => {
        await replication({}, async () => {
          expectUpdateById("1", '{"id":"1","name":"Bobby","age":42,"_deleted":false}').thenReturn()
          await collection.upsert({ id: "1", name: "Bobby", age: 42 })
        })

        expect(await rxdbContents()).toEqual([{ id: "1", name: "Bobby", age: 42 }])
      })

      it("overwrites remote changes without invoking the conflict handler", async () => {
        let conflictHandlerCalled = false
        collection.conflictHandler = {
          isEqual: () => false,
          resolve: (input) => {
            conflictHandlerCalled = true
            return Promise.resolve(input.newDocumentState)
          },
        }

        await replication({}, async () => {
          // Supabase reports zero updated rows, but the default handler does not check for it.
          expectUpdateById("1", '{"id":"1","name":"Bobby","age":42,"_deleted":false}').thenReturn(
            {},
            { "Content-Range": "0-0/0" },
          )
          await collection.upsert({ id: "1", name: "Bobby", age: 42 })
        })

        expect(conflictHandlerCalled).toBe(false)
        expect(await rxdbContents()).toEqual([{ id: "1", name: "Bobby", age: 42 }])
      })

      describe("with postgres error", () => {
        it("automatically retries", async () => {
          const errors = await replication(
            { retryTime: 10 },
            async () => {
              const body = '{"id":"1","name":"Bobby","age":42,"_deleted":false}'
              expectUpdateById("1", body).thenReturnError("53000", 503)
              expectUpdateById("1", body).thenReturn()
              await collection.upsert({ id: "1", name: "Bobby", age: 42 })
            },
            true,
          )
          expect(errors).toHaveLength(1)
        })
      })

      describe("with network error", () => {
        it("automatically retries", async () => {
          const errors = await replication(
            { retryTime: 10 },
            async () => {
              const body = '{"id":"1","name":"Bobby","age":42,"_deleted":false}'
              expectUpdateById("1", body).thenFail()
              expectUpdateById("1", body).thenReturn()
              await collection.upsert({ id: "1", name: "Bobby", age: 42 })
            },
            true,
          )
          expect(errors).toHaveLength(1)
        })
      })
    })

    describe("with custom updateHandler", () => {
      describe("returning true", () => {
        it("does not trigger any queries", async () => {
          await replication({ push: { updateHandler: () => Promise.resolve(true) } }, async () => {
            await collection.upsert({ id: "1", name: "Bobby", age: 42 })
          })

          expect(await rxdbContents()).toEqual([{ id: "1", name: "Bobby", age: 42 }])
        })
      })

      describe("returning false", () => {
        it("invokes conflict handler and updates again", async () => {
          let callCount = 0
          const customUpdateHandler = (
            row: RxReplicationWriteToMasterRow<Human>,
          ): Promise<boolean> => {
            callCount++
            // Only return true (i.e. successful update) if we already fetched the updated state
            return Promise.resolve(row.assumedMasterState?.name === "Bob remote")
          }

          collection.conflictHandler = resolveConflictWithName("Resolved Bob")
          await replication({ push: { updateHandler: customUpdateHandler } }, async () => {
            expectSelectById("1").thenReturn([
              { id: "1", name: "Bob remote", age: 54, _deleted: false, modified_at: "mod" },
            ])
            await collection.upsert({ id: "1", name: "Bobby", age: 42 })
          })

          expect(callCount).toEqual(2)
          expect(await rxdbContents()).toEqual([{ id: "1", name: "Resolved Bob", age: 42 }])
        })
      })
    })
  })

  describe("with client-side delete", () => {
    describe("with default deleted field", () => {
      it("performs UPDATE setting _deleted", async () => {
        const doc = await collection.insert({ id: "1", name: "Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"_deleted":false}').thenReturn()

        await replication({}, async () => {
          expectUpdateById("1", '{"id":"1","name":"Alice","age":null,"_deleted":true}').thenReturn()
          await doc.remove()
        })

        expect(await rxdbContents()).toEqual([])
      })
    })

    describe("with custom deleted field", () => {
      it("performs UPDATE setting the custom deleted field", async () => {
        const doc = await collection.insert({ id: "1", name: "Alice", age: null })
        expectPull().thenReturn([])
        expectInsert('{"id":"1","name":"Alice","age":null,"mydelete":false}').thenReturn()

        await replication({ deletedField: "mydelete" }, async () => {
          expectUpdateById("1", '{"id":"1","name":"Alice","age":null,"mydelete":true}').thenReturn()
          await doc.remove()
        })

        expect(await rxdbContents()).toEqual([])
      })
    })
  })

  describe("with realtime enabled", () => {
    let realtimeSubscription: {
      verifyUnsubscribed: any
      next: (event: Partial<RealtimePostgresChangesPayload<HumanRow>>) => any
    }

    beforeEach(() => {
      const topic = "rxdb-supabase-test"
      realtimeSubscription = supabaseMock.expectRealtimeSubscription("humans", topic)
    })

    describe("without events received", () => {
      it("subscribes to and unsubscribes from RealtimeChannel", async () => {
        expectPull().thenReturn([])
        await replication({ pull: { realtimePostgresChanges: true } }, async () => {
          realtimeSubscription.verifyUnsubscribed.never()
        })
        realtimeSubscription.verifyUnsubscribed.once()
      })
    })

    describe("with insert event received", () => {
      it("inserts new row locally", async () => {
        expectPull().thenReturn([])
        await replication({ pull: { realtimePostgresChanges: true } }, async () => {
          realtimeSubscription.next({
            eventType: "INSERT",
            new: { id: "2", name: "Bob", age: null, _deleted: false, modified_at: "2023-1" },
          })
        })
        expect(await rxdbContents()).toEqual([
          { id: "2", name: "Bob", age: null, modified_at: "2023-1" },
        ])
      })
    })

    describe("with multiple realtime events received", () => {
      it("updates local state", async () => {
        expectPull().thenReturn([])
        await replication({ pull: { realtimePostgresChanges: true } }, async () => {
          realtimeSubscription.next({
            eventType: "INSERT",
            new: { id: "2", name: "Bob", age: null, _deleted: false, modified_at: "2023-1" },
          })
          realtimeSubscription.next({
            eventType: "UPDATE",
            new: { id: "2", name: "Bob", age: 42, _deleted: false, modified_at: "2023-2" },
          })
          realtimeSubscription.next({
            eventType: "INSERT",
            new: { id: "3", name: "Carl", age: null, _deleted: false, modified_at: "2023-3" },
          })
        })
        expect(await rxdbContents()).toEqual([
          { id: "2", name: "Bob", age: 42, modified_at: "2023-2" },
          { id: "3", name: "Carl", age: null, modified_at: "2023-3" },
        ])
      })
    })

    describe("with DELETE event received", () => {
      it("ignores event", async () => {
        expectPull().thenReturn([])
        await replication({ pull: { realtimePostgresChanges: true } }, async () => {
          realtimeSubscription.next({
            eventType: "INSERT",
            new: { id: "1", name: "Alice", age: null, _deleted: false, modified_at: "2023-1" },
          })
          realtimeSubscription.next({
            eventType: "DELETE",
            old: { id: "1", name: "Alice", age: null, _deleted: false, modified_at: "2023-1" },
          })
        })
        expect(await rxdbContents()).toEqual([
          { id: "1", name: "Alice", age: null, modified_at: "2023-1" },
        ])
      })
    })
  })

  describe("with toggleOnDocumentVisible", () => {
    it("defaults to false", () => {
      const state = startReplication({ autoStart: false })
      expect(state.toggleOnDocumentVisible).toBe(false)
    })

    it("passes the option through to RxDB", () => {
      const state = startReplication({ autoStart: false, toggleOnDocumentVisible: true })
      expect(state.toggleOnDocumentVisible).toBe(true)
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
      supabaseClient: supabaseMock.client,
      collection,
      push: {},
      ...options,
      pull: {
        realtimePostgresChanges: false,
        lastModifiedField: MODIFIED_FIELD,
        ...options.pull,
      },
    })
  }

  const expectPull = (
    options: {
      limit?: number
      modifiedAfter?: string
      modifiedField?: string
      fetchDeleted?: boolean
      filters?: [string, string][]
    } = {},
  ) => {
    const modifiedField = options.modifiedField || MODIFIED_FIELD
    const params: [string, string][] = [["select", "*"]]
    if (!options.fetchDeleted) params.push(["is_deleted", "eq.false"])
    params.push(...(options.filters || []))
    if (options.modifiedAfter !== undefined) {
      params.push(["or", `(${modifiedField}.gt.${JSON.stringify(options.modifiedAfter)})`])
    }
    params.push(["order", `${modifiedField}.asc,id.asc`])
    params.push(["limit", `${options.limit || 1000}`])
    const query = new URLSearchParams(params).toString()
    return supabaseMock.expectQuery(`Pull query ${query}`, { table: "humans", params: query })
  }

  const expectSelectById = (id: string) => {
    return supabaseMock.expectQuery(`Select by id ${id}`, {
      table: "humans",
      params: `select=*&id=eq.${id}&limit=1`,
    })
  }

  const expectInsert = (body: string) => {
    return supabaseMock.expectInsert("humans", body)
  }

  const expectUpdateById = (id: string, body: string) => {
    return supabaseMock.expectQuery(`UPDATE ${id}: ${body}`, {
      method: "PATCH",
      table: "humans",
      params: `id=eq.${id}`,
      body,
    })
  }

  const rxdbContents = async (): Promise<Human[]> => {
    const results = await collection.find().exec()
    return results.map((doc) => doc.toJSON())
  }

  const createHumans = (count: number): HumanRow[] => {
    return Array.from(Array(count).keys()).map((id) => createHuman(id + 1))
  }

  const createHuman = (id: number): HumanRow => {
    return {
      id: id.toString(),
      name: `Human ${id}`,
      age: id % 2 == 0 ? null : (id * 11) % 150, // Schema allows a maximum age of 150
      _deleted: false,
      modified_at: "2023-" + id,
    }
  }

  afterEach(async () => {
    supabaseMock.verifyNoMoreQueriesExpected()
    await db.remove()
  })
})
