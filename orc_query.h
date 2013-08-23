/*
 * orc_query.h
 *
 *  Created on: Aug 23, 2013
 *      Author: gokhan
 */

#ifndef ORC_QUERY_H_
#define ORC_QUERY_H_

#include "postgres.h"
#include "orc_fdw.h"

#include "catalog/pg_type.h"
#include "nodes/relation.h"

typedef enum
{
	ORC_QUERY_EQ = 0,
	ORC_QUERY_LT = 1,
	ORC_QUERY_GT = 2,
	ORC_QUERY_LTE = 3,
	ORC_QUERY_GTE = 4,
	ORC_QUERY_NE = 5
} OrcQueryOperator;

List* ApplicableOpExpressionList(RelOptInfo *baserel);

#endif /* ORC_QUERY_H_ */
