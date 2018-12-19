'use strict'

const { of } = require('rxjs');
const { map } = require('rxjs/operators');
const { CustomError, DefaultError } = require('./customError');

const buildSuccessResponse$ = (rawRespponse) => {
    return of(rawRespponse).pipe(
        map(resp => {
            return {
                data: resp,
                result: {
                    code: 200
                }
            };
        })
    );
};

const buildErrorResponse$ = (errCode, rawRespponse) => {
    return of(rawRespponse).pipe(
        map(resp => {
            return {
                data: resp,
                result: {
                    code: errCode
                }
            };
        })
    );
};

const handleError$ = (err) => {
    return of(err).pipe(
        map(err => {
            const exception = { data: null, result: {} };
            const isCustomError = err instanceof CustomError;
            if (!isCustomError) {
                err = new DefaultError(err);
            }
            exception.result = {
                code: err.code,
                error: { ...err.getContent() }
            };
            return exception;
        })
    );
}

module.exports = {
    buildSuccessResponse$, 
    handleError$,
    buildErrorResponse$
}