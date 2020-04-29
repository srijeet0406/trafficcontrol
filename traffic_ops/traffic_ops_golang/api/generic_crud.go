package api

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/apache/trafficcontrol/grove/web"
	"github.com/apache/trafficcontrol/lib/go-log"
	"github.com/apache/trafficcontrol/lib/go-rfc"
	ims2 "github.com/apache/trafficcontrol/traffic_ops/traffic_ops_golang/util/ims"
	"net/http"

	"github.com/apache/trafficcontrol/lib/go-tc"
	"github.com/apache/trafficcontrol/lib/go-util"
	"github.com/apache/trafficcontrol/traffic_ops/traffic_ops_golang/dbhelpers"
)

type GenericCreator interface {
	GetType() string
	APIInfo() *APIInfo
	SetKeys(map[string]interface{})
	SetLastUpdated(tc.TimeNoMod)
	InsertQuery() string
}

type GenericReader interface {
	GetType() string
	APIInfo() *APIInfo
	ParamColumns() map[string]dbhelpers.WhereColumnInfo
	NewReadObj() interface{}
	SelectQuery() string
	SelectMaxLastUpdatedQuery(where string, orderBy string, pagination string, tableName string) string
}

type GenericUpdater interface {
	GetType() string
	APIInfo() *APIInfo
	SetLastUpdated(tc.TimeNoMod)
	UpdateQuery() string
}

type GenericDeleter interface {
	GetType() string
	APIInfo() *APIInfo
	DeleteQuery() string
	InsertIntoDeletedQuery() string
}

// GenericOptionsDeleter can use any key listed in DeleteKeyOptions() to delete a resource.
type GenericOptionsDeleter interface {
	GetType() string
	APIInfo() *APIInfo
	DeleteKeyOptions() map[string]dbhelpers.WhereColumnInfo
	DeleteQueryBase() string
}

// GenericCreate does a Create (POST) for the given GenericCreator object and type. This exists as a generic function, for the common use case of a single "id" key and a lastUpdated field.
func GenericCreate(val GenericCreator) (error, error, int) {
	resultRows, err := val.APIInfo().Tx.NamedQuery(val.InsertQuery(), val)
	if err != nil {
		return ParseDBError(err)
	}
	defer resultRows.Close()

	id := 0
	lastUpdated := tc.TimeNoMod{}
	rowsAffected := 0
	for resultRows.Next() {
		rowsAffected++
		if err := resultRows.Scan(&id, &lastUpdated); err != nil {
			return nil, errors.New(val.GetType() + " create scanning: " + err.Error()), http.StatusInternalServerError
		}
	}
	if rowsAffected == 0 {
		return nil, errors.New(val.GetType() + " create: no " + val.GetType() + " was inserted, no id was returned"), http.StatusInternalServerError
	} else if rowsAffected > 1 {
		return nil, errors.New("too many ids returned from " + val.GetType() + " insert"), http.StatusInternalServerError
	}
	val.SetKeys(map[string]interface{}{"id": id})
	val.SetLastUpdated(lastUpdated)
	return nil, nil, http.StatusOK
}

// GenericCreateNameBasedID does a Create (POST) for the given GenericCreator object and type. This exists as a generic function, for the use case of a single "name" key (not a numerical "id" key) and a lastUpdated field.
func GenericCreateNameBasedID(val GenericCreator) (error, error, int) {
	resultRows, err := val.APIInfo().Tx.NamedQuery(val.InsertQuery(), val)
	if err != nil {
		return ParseDBError(err)
	}
	defer resultRows.Close()

	lastUpdated := tc.TimeNoMod{}
	rowsAffected := 0
	for resultRows.Next() {
		rowsAffected++
		if err := resultRows.Scan(&lastUpdated); err != nil {
			return nil, errors.New(val.GetType() + " create scanning: " + err.Error()), http.StatusInternalServerError
		}
	}
	if rowsAffected == 0 {
		return nil, errors.New(val.GetType() + " create: no " + val.GetType() + " was inserted, no row was returned"), http.StatusInternalServerError
	} else if rowsAffected > 1 {
		return nil, errors.New("too many rows returned from " + val.GetType() + " insert"), http.StatusInternalServerError
	}
	val.SetLastUpdated(lastUpdated)
	return nil, nil, http.StatusOK
}

func MakeFirstQuery(val GenericReader, h map[string][]string, where string, orderBy string, pagination string, queryValues map[string]interface{}) bool {
	ims := []string{}
	runSecond := true
	if h == nil {
		return runSecond
	}
	ims = h[rfc.IfModifiedSince]
	if ims == nil || len(ims) == 0 {
		return runSecond
	}
	if l, ok := web.ParseHTTPDate(ims[0]); !ok {
		return runSecond
	} else {
		query := val.SelectMaxLastUpdatedQuery(where, orderBy, pagination, val.GetType())
		rows, err := val.APIInfo().Tx.NamedQuery(query, queryValues)
		defer rows.Close()
		if err != nil {
			log.Warnf("Couldn't get the max last updated time: %v", err)
			return runSecond
		}
		if err == sql.ErrNoRows {
			runSecond = false
			return runSecond
		}
		// This should only ever contain one row
		if rows.Next() {
			v := &ims2.LatestTimestamp{}
			if err = rows.StructScan(v); err != nil || v == nil {
				log.Warnf("Failed to parse the max time stamp into a struct %v", err)
				return runSecond
			}
			// The request IMS time is later than the max of (lastUpdated, deleted_time)
			if l.After(v.LatestTime.Time) {
				runSecond = false
				return runSecond
			}
		} else {
			runSecond = false
		}
	}
	return runSecond
}

func GenericRead(h http.Header, val GenericReader) ([]interface{}, error, error, int) {
	vals := []interface{}{}
	code := http.StatusOK
	where, orderBy, pagination, queryValues, errs := dbhelpers.BuildWhereAndOrderByAndPagination(val.APIInfo().Params, val.ParamColumns())
	if len(errs) > 0 {
		return nil, util.JoinErrs(errs), nil, http.StatusBadRequest
	}
	runSecond := MakeFirstQuery(val, h, where, orderBy, pagination, queryValues)
	if !runSecond {
		code = http.StatusNotModified
		return vals, nil, nil, code
	}
	// Case where we need to run the second query
	query := val.SelectQuery() + where + orderBy + pagination
	rows, err := val.APIInfo().Tx.NamedQuery(query, queryValues)
	if err != nil {
		return nil, nil, errors.New("querying " + val.GetType() + ": " + err.Error()), http.StatusInternalServerError
	}
	defer rows.Close()

	for rows.Next() {
		v := val.NewReadObj()
		if err = rows.StructScan(v); err != nil {
			return nil, nil, errors.New("scanning " + val.GetType() + ": " + err.Error()), http.StatusInternalServerError
		}
		vals = append(vals, v)
	}
	return vals, nil, nil, code
}

// GenericUpdate handles the common update case, where the update returns the new last_modified time.
func GenericUpdate(val GenericUpdater) (error, error, int) {
	rows, err := val.APIInfo().Tx.NamedQuery(val.UpdateQuery(), val)
	if err != nil {
		return ParseDBError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return errors.New("no " + val.GetType() + " found with this id"), nil, http.StatusNotFound
	}
	lastUpdated := tc.TimeNoMod{}
	if err := rows.Scan(&lastUpdated); err != nil {
		return nil, errors.New("scanning lastUpdated from " + val.GetType() + " insert: " + err.Error()), http.StatusInternalServerError
	}
	val.SetLastUpdated(lastUpdated)
	if rows.Next() {
		return nil, errors.New(val.GetType() + " update affected too many rows: >1"), http.StatusInternalServerError
	}
	return nil, nil, http.StatusOK
}

// GenericOptionsDelete does a Delete (DELETE) for the given GenericOptionsDeleter object and type. Unlike
// GenericDelete, there is no requirement that a specific key is used as the parameter.
// GenericOptionsDeleter.DeleteKeyOptions() specifies which keys can be used.
func GenericOptionsDelete(val GenericOptionsDeleter) (error, error, int) {
	where, _, _, queryValues, errs := dbhelpers.BuildWhereAndOrderByAndPagination(val.APIInfo().Params, val.DeleteKeyOptions())
	if len(errs) > 0 {
		return util.JoinErrs(errs), nil, http.StatusBadRequest
	}

	query := val.DeleteQueryBase() + where
	tx := val.APIInfo().Tx
	result, err := tx.NamedExec(query, queryValues)
	if err != nil {
		return ParseDBError(err)
	}

	if rowsAffected, err := result.RowsAffected(); err != nil {
		return nil, errors.New("deleting " + val.GetType() + ": getting rows affected: " + err.Error()), http.StatusInternalServerError
	} else if rowsAffected < 1 {
		return errors.New("no " + val.GetType() + " with that key found"), nil, http.StatusNotFound
	} else if rowsAffected > 1 {
		return nil, fmt.Errorf(val.GetType()+" delete affected too many rows: %d", rowsAffected), http.StatusInternalServerError
	}

	return nil, nil, http.StatusOK
}

func InsertInDeletedTable(val GenericDeleter) (error, error, int) {
	query := val.InsertIntoDeletedQuery()
	log.Debugf("InsertInDeletedTable query is %v", query)
	result, err := val.APIInfo().Tx.NamedExec(query, val)
	if err != nil {
		log.Warnf("DB error while inserting into deleted table %v", err)
		return ParseDBError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err, nil, http.StatusInternalServerError
	}
	if rowsAffected != 1 {
		log.Warnf("Warning: %v rows were affected by this operation, expected was 1", rowsAffected)
	}
	return nil, nil, http.StatusOK
}

// GenericDelete does a Delete (DELETE) for the given GenericDeleter object and type. This exists as a generic function, for the common use case of a simple delete with query parameters defined in the sqlx struct tags.
func GenericDelete(val GenericDeleter) (error, error, int) {
	code := http.StatusOK
	e1, e2, code := InsertInDeletedTable(val)
	if e1 != nil || e2 != nil {
		return e1, e2, code
	}
	result, err := val.APIInfo().Tx.NamedExec(val.DeleteQuery(), val)
	if err != nil {
		return ParseDBError(err)
	}

	if rowsAffected, err := result.RowsAffected(); err != nil {
		return nil, errors.New("deleting " + val.GetType() + ": getting rows affected: " + err.Error()), http.StatusInternalServerError
	} else if rowsAffected < 1 {
		return errors.New("no " + val.GetType() + " with that key found"), nil, http.StatusNotFound
	} else if rowsAffected > 1 {
		return nil, fmt.Errorf(val.GetType()+" delete affected too many rows: %d", rowsAffected), http.StatusInternalServerError
	}
	return nil, nil, code
}
