import * as PG from 'pg'
import { quoteIdent, quoteLiteral } from 'node-pg-format'
import * as Koa from 'koa'
import { parse } from 'url'
import * as T from '@symbion/runtype'

import { ServerError } from './utils.js'

// Utility functions
export function ql(v?: unknown) {
	return v === null ? 'NULL'
		: Array.isArray(v) ? quoteLiteral('' + v).replace(/^'(.*)'$/, (match, p1) => `'{${p1}}'`)
		: quoteLiteral('' + v)
}

interface InitOpts {
	url: string
	max?: number
}

interface QueryOpts {
	includeNulls?: boolean
	noTrimStrings?: boolean
}

export class DB {
	pg: PG.ClientBase
	constructor(pg: PG.ClientBase) {
		this.pg = pg
	}

	transformResult(res: PG.QueryResult, opts: QueryOpts = {}) {
		for (const row of res.rows) {
			for (const key in row) {
				if (row[key] == null && !opts.includeNulls) {
					delete row[key]
				} else if (typeof row[key] == 'string' && !opts.noTrimStrings) {
					row[key] = row[key].trim()
				}
			}
		}
	}

	begin() {
		return this.pg.query('BEGIN')
	}

	commit() {
		return this.pg.query('COMMIT')
	}

	rollback() {
		return this.pg.query('ROLLBACK')
	}

	async exec<T extends PG.QueryResultRow = any>(query: string, args: unknown[] = [], opts: QueryOpts = {}): Promise<PG.QueryResult<T>> {
		const t = Date.now()
		const q = query.replace(/^!/, '').replace(/\n[\t ]*/g, '\n\t')
		try {
			if (!query.match(/^!/)) console.log('Q: ' + q, args || '')
			const res = await this.pg.query(q, args)
			this.transformResult(res, opts)
			if (!query.match(/^!/)) console.log(`R: ${res.rows.length} rows ${Date.now() - t} ms`)
			return res as PG.QueryResult<T>
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				console.log('DB:EXEC ERROR', e)
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				console.log('E: ' + q, args || '')
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			} else throw err
		}
	}

	async get<T = any>(query: string, args: unknown[] = [], opts: QueryOpts = {}): Promise<T | undefined> {
		const t = Date.now()
		const q = query.replace(/^!/, '').replace(/\n[\t ]*/g, '\n\t')
		try {
			if (!query.match(/^!/)) console.log('Q: ' + q, args || '')
			const res = await this.pg.query(q, args)
			this.transformResult(res, opts)
			if (!query.match(/^!/)) console.log(`R: ${res.rows.length} rows ${Date.now() - t} ms`)
			if (res.rows.length > 1) throw new Error('Internal error: db.get() result')
			return res.rows[0] as T | undefined
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				console.log('DB:EXEC ERROR', e)
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				console.log('E: ' + q, args || '')
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			} else throw err
		}
	}

	async proc(query: string, args: unknown[] = []) {
		const t = Date.now()
		const q = query.replace(/^!/, '').replace(/\n[\t ]*/g, '\n\t')
		try {
			if (!query.match(/^!/)) console.log('P: ' + q, args || '')
			const res = await this.pg.query(q, args)
			if (!query.match(/^!/)) console.log(`R: ${res.rowCount} rows ${Date.now() - t} ms`)
			return res.rowCount
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				console.log('E: ' + q, args || '')
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			} else throw err
		}
	}

	async func<T = any>(query: string, args: unknown[] = []): Promise<T> {
		const t = Date.now()
		const q = query.replace(/^!/, '').replace(/\n[\t ]*/g, '\n\t')
		try {
			if (!query.match(/^!/)) console.log('F: ' + q, args || '')
			const res = await this.pg.query({text: q, values: args, rowMode: 'array'})
			if (!query.match(/^!/)) console.log(`R: ${res.rows[0][0]} ${Date.now() - t} ms`)
			if (res.rows.length != 1 || res.rows[0].length != 1) throw new Error('Internal error: db.func() result')
			return res.rows[0][0]
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				console.log('E: ' + q, args || '')
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			} else throw err
		}
	}

	async map(query: string, key: string, args: unknown[] = []) {
		const t = Date.now()
		const q = query.replace(/^!/, '').replace(/\n[\t ]*/g, '\n\t')
		try {
			if (!query.match(/^!/)) console.log('Q: ' + q, args || '')
			const res = await this.pg.query(q, args)
			this.transformResult(res)
			if (!query.match(/^!/)) console.log(`R: ${res.rows.length} rows ${Date.now() - t} ms`)
			const ret = res.rows.reduce((acc, item) => {
				acc[item[key]] = item
				return acc
			}, {} as Record<string, Object>)
			return ret
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				console.log('DB:MAP ERROR', e)
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				console.log('E: ' + q, args || '')
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			} else throw err
		}
	}

	async insert<T, KEYS extends keyof T, GK extends KEYS>(table: string, schema: T.Schema<T, KEYS, GK>, data: T) {
		try {
			const allFlds = (Object.keys(schema.props) as (keyof T)[]).filter(f => {
				const prop = schema.props[f]
				return prop.type && prop.dbName !== null
			})
			const flds = (Object.keys(schema.props) as (keyof T)[]).filter(f => {
				const prop = schema.props[f]
				return prop.type && prop.dbName !== null && !schema.keys.includes(f as KEYS) && data[f] !== undefined
			})
			const keys = schema.keys.filter(f => data[f] != null)

			const query = `INSERT INTO ${table} (${[...keys, ...flds].map(f => quoteIdent((schema.props[f] as any)?.dbName || f)).join(', ')}) `
				+ `VALUES (${[...keys, ...flds].map(f => ql(data[f])).join(', ')}) `
				+ 'RETURNING ' + [...keys, ...flds].map(f => quoteIdent((schema.props[f] as any)?.dbName || f)).join(', ')
			console.log('Query', query)
			let res = await this.pg.query(query, [])
			return res.rows[0]
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			}
		}
	}

	async upsert<T, KEYS extends keyof T, GK extends KEYS>(table: string, schema: T.Schema<T, KEYS, GK>, data: T) {
		try {
			const flds = (Object.keys(schema.props) as (keyof T)[]).filter(f => {
				const prop = schema.props[f]
				return prop.type && prop.dbName !== null && !schema.keys.includes(f as KEYS) && data[f] !== undefined
			})
			const keys = schema.keys
			if (keys.length < 1) throw new Error('Key missing in schema definition')

			const query = `INSERT INTO ${table} (${[...keys, ...flds].map(f => quoteIdent((schema.props[f] as any)?.dbName || f)).join(', ')}) `
				+ `VALUES (${[...keys, ...flds].map(f => ql(data[f])).join(', ')}) `
				+ `ON CONFLICT (${keys.join(',')}) DO `
				+ `UPDATE SET ${flds.map(f => quoteIdent((schema.props[f] as any)?.dbName || f) + '=' + ql(data[f])).join(', ')}`
				+ 'RETURNING ' + flds.map(f => quoteIdent((schema.props[f] as any)?.dbName || f)).join(', ')
			console.log('Query', query)
			let res = await this.pg.query(query, [])
			return res.rows[0]
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			}
		}
	}

	async update<T, KEYS extends keyof T, GK extends KEYS>(table: string, schema: T.Schema<T, KEYS, GK>, data: T) {
		try {
			const flds = (Object.keys(schema.props) as (keyof T)[]).filter(f => {
				const prop = schema.props[f]
				return prop.type && prop.dbName !== null && !schema.keys.includes(f as KEYS) && data[f] !== undefined
			})
			const keys = schema.keys
			if (keys.length < 1) throw new Error('Key missing in schema definition')

			const query = `UPDATE ${table} SET `
				+ flds.map(f => quoteIdent((schema.props[f] as any)?.dbName || f) + '=' + ql(data[f])).join(', ')
				+ ' WHERE '
				+ keys.map(f => quoteIdent((schema.props[f] as any)?.dbName || f) + (data[f] === null ? 'ISNULL' : '=' + ql(data[f]))).join(' AND ')
				+ ' RETURNING ' + flds.map(f => quoteIdent((schema.props[f] as any)?.dbName || f)).join(', ')
			console.log('Query', query)
			let res = await this.pg.query(query, [])
			console.log(res.rows[0])
			return res.rows[0]
		} catch (err) {
			if (err instanceof Error) {
				const e = err.toString()
				const [_, code, descr] = e.match(/^[^@]*@([A-Z0-9-]+) *(.*)$/) || []
				if (code) throw new ServerError(code, descr)
				else throw new Error(e)
			}
		}
	}
}

export interface Context extends Koa.Context {
	pgPool: PG.Pool
	db: DB
}

// Koa middleware to access DB Pool
//export async function pgMiddleware<Context extends Koa.Context & { db?: DB }, Next extends Koa.Next>(ctx: Context, next: Next) {
export async function pgMiddleware<CTX extends Context, Next extends Koa.Next>(ctx: CTX, next: Next) {
	if (ctx.db) return next()

	let pg = await ctx.pgPool.connect()
	ctx.db = new DB(pg)
	ctx.body = {}
	try {
		await next()
		pg.release()
	} catch (err) {
		pg.release()
		throw err
	}
	delete (ctx as any).db
}

export async function init<ST extends Koa.DefaultState, CTX extends Context>(app: Koa<ST, CTX>, { url, max }: InitOpts) {
	const params = parse(url)
	const [user, password] = (params.auth || '').split(':')
	const dbConfig = {
		user,
		password,
		host: params.hostname || undefined,
		port: +(params.port || 5432),
		database: (params.pathname || '').split('/')[1],
		max
	}
	app.context.pgPool = new PG.Pool(dbConfig)
	app.context.pgPool.on('connect', function onConnect(conn: PG.ClientBase) {
		conn.on('error', console.error)
		conn.on('notice', function onNotice(notice) {
			console.log('NOTICE', notice.message)
		})
	})
}

// vim: ts=4
