export class ServerError extends Error {
	errCode: string
	errStr: string
	httpStatus: number

	constructor(errCode: string, errStr: string, httpStatus: number = 400) {
		super(errStr)
		this.errCode = errCode
		this.errStr = errStr
		this.httpStatus = httpStatus
	}
}

// vim: ts=4
