import { WithDeleted } from "rxdb"

export interface Human {
  id: string
  name: string
  age: number | null
  modified_at?: string
}

export type HumanRow = WithDeleted<Human> & {
  modified_at: string
}

/**
 * Name of the last modified column used by the tests. The replication only keeps columns that are
 * part of the collection schema and don't start with an underscore, so the default `_modified`
 * column cannot be used.
 */
export const MODIFIED_FIELD = "modified_at"

export const HUMAN_SCHEMA = {
  title: "human schema",
  version: 0,
  primaryKey: "id",
  type: "object",
  properties: {
    id: {
      type: "string",
      maxLength: 100,
    },
    name: {
      type: "string",
    },
    age: {
      description: "age in years",
      type: ["integer", "null"],
      minimum: 0,
      maximum: 150,
      multipleOf: 1,
    },
    modified_at: {
      type: "string",
    },
  },
  required: ["name", "id", "age"],
}
