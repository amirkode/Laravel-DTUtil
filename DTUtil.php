<?php
/**
 * credit to @amirkode (github)
 * reach me on amirmb.com
 * 22-12-2020
 *
 * Contains DataTable Helper Functions for Laravel Framework
 * All parameters are based on https://datatables.net/manual/server-side
 * All functions are based on Laravel Frameworks (it strictly won't work on plain PHP)
 *
 *
 * DTUtil (DataTable Utility) version 1.0.0
 * @license MIT for DataTable http://datatables.net/license_mit
 * @license MIT for Laravel https://laravel-guide.readthedocs.io/en/latest/license/
 */
namespace AmirKode\LaravelDTUtil;

use Illuminate\Support\Facades\DB;
use Illuminate\Http\Request;

class DTUtil {
    /**
     *  These two variables($elqModel and $qbModel) are the options how the query executed
     *
     * @var Model/Builder/string    $mainQuery      this can be Eloquent Model Instance, Query Builder Instance, or Main Query String
     * @var int                     $queryType      Query type denotes the type of query chosen
     */


    // Convert as json Flag
    public $outputAsJson = true;

    private $mainQuery = null;
    private $queryType = self::QUERY_TYPE_PLAIN;

    // saved properties for Plain Query
    private $limit = null;
    private $order = null;
    private $filter = null;

    private const QUERY_TYPE_ELOQUENT = 1; // query uses eloquent model
    private const QUERY_TYPE_QUERY_BUILDER = 2; // query uses query builder
    private const QUERY_TYPE_PLAIN = 3; // query uses plain query


    /**
     * DTUtil constructor.
     * Predefined data can be pass through the constructor
     *
     * @param null $queryType             Predefined query type
     * @param null $mainQuery             Predefined query/models (a must if $queryType passed)
     *
     */
    public function __construct($queryType = null, $mainQuery = null) {
        if(!empty($queryType)) {
            $this->queryType = $queryType;
            $this->mainQuery = $mainQuery;
        }
    }

    /**
     * Generate plain search filters for the query
     *
     * @param @see query_filter
     * @return string
     */

    private function make_filters($r, $columns, $globalSearchIndexes, $specifiedSearchIndexes,
                                  $nonCaseSensitiveLike, $toStringCaster) {
        // Request Columns
        $rColumns = $r->columns;

        // Query Columns
        $dbColumns = array_column($columns, 'col');
        $aliasColumns = array_column($columns, 'alias');

        // To String column caster
        $columnCaster = $toStringCaster['caster'];
        $columnCasterSpecifier = $toStringCaster['specifier'];

        // global search keyword
        $globalSearchValue = $r->search['value'];

        // set filters/conditions to Plain Query String
        $globalSearch = [];
        $specifiedSearch = [];
        $filter = '';

        // global search filtering (filter to all columns)
        foreach ($globalSearchIndexes as $colInd) {
            $useColumnPrefix = !empty($aliasColumns[$colInd]);
            $cond = str_replace($columnCasterSpecifier, ($useColumnPrefix ? $aliasColumns[$colInd] . '.' : '') .
                    $dbColumns[$colInd], $columnCaster) . ' ' . $nonCaseSensitiveLike . ' \'%' . $globalSearchValue . '%\'';
            // push condition to array of global search
            array_push($globalSearch, $cond);
        }

        // specified search filtering
        foreach($specifiedSearchIndexes as $ind) {
            $useColumnPrefix = !empty($aliasColumns[$ind['col_ind']]);
            $search = $rColumns[$ind['org_ind']]['search']['value'];
            $cond = str_replace($columnCasterSpecifier, ($useColumnPrefix ? $aliasColumns[$ind['col_ind']] . '.' : '') .
                    $dbColumns[$ind['col_ind']], $columnCaster) . ' ' . $nonCaseSensitiveLike . ' \'%' . $search . '%\'';
            // push condition to array of specified search
            array_push($specifiedSearch, $cond);
        }

        // create where condition of specified search
        if(!empty($globalSearch))
            $filter = 'WHERE (' . implode(' OR ', $globalSearch) . ')';

        // create where condition for specified search
        if(!empty($specifiedSearch)) {
            if($filter == '')
                $filter = 'WHERE (' . implode(' AND ', $specifiedSearch) . ')';
            else
                $filter = $filter . ' AND ' . implode(' AND ', $specifiedSearch);
        }

        return $filter;
    }

    /**
     * Set filters for the query (mostly for search request)
     *
     * filtering provided for Global Search and Specified Column Search
     * this function currently uses Request['column'] as the only parameter for filtering
     * to match the front end column and the column on the database table, this function match the Request['column'][index]['data'].
     * the property 'dt' in $columns item must refer to the correct Request['column'][index]['data']
     *
     * @param @see complex_res() for full explanation
     */
    private function query_filter($r, $startColumn, $columns, $nonCaseSensitiveLike, $toStringCaster, $condSpecifier = null) {
        if(empty($r->columns)) {
            if($this->queryType == self::QUERY_TYPE_PLAIN)
                $this->mainQuery = str_replace($condSpecifier, '',$this->mainQuery);
            // no need to add any statements for non Plain Query Type
            return;
        }

        // Request Columns
        $rColumns = $r->columns;

        // Query Columns
        $dbColumns = array_column($columns, 'col');
        $dtColumns = array_column($columns, 'dt');

        // search filter indexes
        $globalSearchIndexes = [];
        $specifiedSearchIndexes = [];
        $globalSearchValue = $r->search['value'];

        // global search filtering (filter to all columns)
        if(!empty($r->search) && $globalSearchValue != '') {
            // iterate all request columns
            for($i = $startColumn; $i < count($rColumns); $i ++) {
                $colInd = array_search($rColumns[$i]['data'], $dtColumns);
                // push condition to array of global search column indexes
                if($colInd !== false)
                   array_push($globalSearchIndexes, $colInd);
            }
        }

        // specified search filtering
        for($i = $startColumn; $i < count($rColumns); $i ++) {
            $search = $rColumns[$i]['search']['value'];
            // check if the current column is searchable
            if(!empty($search) && $rColumns[$i]['searchable'] == 'true') {
                $colInd = array_search($rColumns[$i]['data'], $dtColumns);
                // push condition to array of specified search column indexes
                // key value 'col_ind' denotes column index on database
                // key value 'org_ind' denotes original column index sent by datatable
                if($colInd !== false)
                    array_push($specifiedSearchIndexes, ['col_ind' => $colInd, 'org_ind' => $i]);
            }
        }

        if($this->queryType == self::QUERY_TYPE_ELOQUENT || $this->queryType == self::QUERY_TYPE_QUERY_BUILDER) {
            // set filters/conditions to Eloquent Instance or Query Builder Instance
            if(!empty($specifiedSearchIndexes)) {
                if(!empty($globalSearchIndexes)) {
                    $this->mainQuery = $this->mainQuery->where(function($q) use (&$colInd, &$dbColumns,
                                &$globalSearchIndexes, &$globalSearchValue, &$nonCaseSensitiveLike) {
                        $firstCond = true;
                        foreach ($globalSearchIndexes as $colInd) {
                            if($firstCond) {
                                $q = $q->where($dbColumns[$colInd], $nonCaseSensitiveLike, '%' . $globalSearchValue . '%');
                                $firstCond = !$firstCond;
                            } else
                                $q = $q->orWhere($dbColumns[$colInd], $nonCaseSensitiveLike, '%' . $globalSearchValue . '%');
                        }
                    });
                }

                // specified search filtering
                foreach ($specifiedSearchIndexes as $ind) {
                    $search = $rColumns[$ind['org_ind']]['search']['value'];
                    $this->mainQuery = $this->mainQuery->where($dbColumns[$ind['col_ind']], $nonCaseSensitiveLike, '%' .
                                        $search . '%');
                }
            } else if(!empty($globalSearchIndexes)) {
                // global search filtering (filter to all columns)
                $firstCond = true;
                foreach ($globalSearchIndexes as $colInd) {
                    if ($firstCond) {
                        $this->mainQuery = $this->mainQuery->where($dbColumns[$colInd], $nonCaseSensitiveLike, '%' .
                                            $globalSearchValue . '%');
                        $firstCond = !$firstCond;
                    } else
                        $this->mainQuery = $this->mainQuery->orWhere($dbColumns[$colInd], $nonCaseSensitiveLike, '%' .
                                            $globalSearchValue . '%');
                }
            }

            $this->filter = $this->make_filters($r, $columns, $globalSearchIndexes, $specifiedSearchIndexes, $nonCaseSensitiveLike, $toStringCaster);
        } else if($this->queryType == self::QUERY_TYPE_PLAIN && !empty($condSpecifier)) {
            // set filters/conditions to Plain Query String
            $filter = $this->make_filters($r, $columns, $globalSearchIndexes, $specifiedSearchIndexes, $nonCaseSensitiveLike, $toStringCaster);
            $this->filter = $filter;
            $this->mainQuery = str_replace($condSpecifier, $filter, $this->mainQuery);
        }
    }

    /**
     * Set Ordering for the query
     * in the plain query, preordered query by certain column is possible to perform.
     * $preOrdered flag denotes that the query is preordered or otherwise
     *
     * @param @see complex_res() for full explanation
     */
    private function query_order(Request $r, $startColumn, $columns, $orderSpecifier = null, $preOrdered = false) {
        // Request Columns
        $rColumns = $r->columns;
        // Request Orders
        $rOrders = $r->order;

        // Query Columns
        $dbColumns = array_column($columns, 'col');
        $dtColumns = array_column($columns, 'dt');
        $aliasColumns = array_column($columns, 'alias');

        // indexes
        $orderIndexes = [];

        if(!empty($rOrders)) {
            for($i = 0; $i < count($rOrders); $i ++) {
                $colInd = intval($rOrders[$i]['column']);
                $rColumn = $rColumns[$colInd];
                $colInd = array_search($rColumn['data'], $dtColumns);
                $orderType = $rOrders[$i]['dir'] == 'asc' ? 'ASC' : 'DESC';

                if($rColumn['orderable'] == 'true' && $colInd !== false && $dtColumns[$colInd] >= $startColumn) {
                    // push ordering and index to array of order indexes
                    // key value 'col_ind' denotes column index on database
                    // key value 'order_type' denotes type of column order ('ASC' or 'DESC')
                    array_push($orderIndexes, ['col_ind' => $colInd, 'order_type' => $orderType]);
                }
            }
        }

        if($this->queryType == self::QUERY_TYPE_ELOQUENT || $this->queryType == self::QUERY_TYPE_QUERY_BUILDER) {
            // set ordering to Eloquent Instance or Query Builder Instance
            if(!empty($orderIndexes)) {
                foreach($orderIndexes as $orderIndex)
                    $this->mainQuery = $this->mainQuery->orderBy($dbColumns[$orderIndex['col_ind']],
                                        $orderIndex['order_type']);
            }
        } else if($this->queryType == self::QUERY_TYPE_PLAIN) {
            // set filters/conditions to Plain Query String
            if(!empty($orderIndexes)) {
                $orders = [];

                for($i = 0; $i < count($orderIndexes); $i ++) {
                    $ind = $orderIndexes[$i]['col_ind'];
                    $useColumnPrefix = !empty($aliasColumns[$ind]);
                    $currOrder = ($useColumnPrefix ? $aliasColumns[$ind] . '.' . $dbColumns[$ind] : $dbColumns[$ind]) .
                                    ' ' . $orderIndexes[$i]['order_type'];
                    // push current column order to $orders
                    array_push($orders, $currOrder);
                }

                $order = ($preOrdered ? ', ' : 'ORDER BY ') . implode(', ', $orders);
                $this->order = $order;
                $this->mainQuery = str_replace($orderSpecifier, $order, $this->mainQuery);
            } else
                $this->mainQuery = str_replace($orderSpecifier, '', $this->mainQuery);
        }
    }

    /**
     * Set limit for the query
     * Limit indicates how many data will be returned in certain offsets
     *
     * * warning **
     *   for plain query, uses common sql syntax e.g MySql, Postgresql, etc.
     *   SQL server might use predefined limitation e.g "SELECT TOP ..."
     *   Fill the limit using $limitSpecifier and $additional specifier provided for the offset
     *   These two specifiers is not strictly filled in the same order (can put actual specifier in any order)
     *   But, $limitSpecifier will be replaced by $r->length and $additionalSpecifier will be replaced by $r->start
     *
     * @param @see complex_res() for full explanation
     */
    private function query_limit($r, $limitSpecifier = null, $additionalSpecifer = null) {
        if($r->start != null && $r->length != -1 && !empty($this->mainQuery)) {
            if($this->queryType == self::QUERY_TYPE_ELOQUENT) {
                $this->mainQuery = $this->mainQuery->skip($r->start)->take($r->length);
            } else if($this->queryType == self::QUERY_TYPE_QUERY_BUILDER) {
                $this->mainQuery = $this->mainQuery->offset($r->start)->limit($r->length);
            } else if($this->queryType == self::QUERY_TYPE_PLAIN && !empty($limitSpecifier)) {
                if(empty($additionalSpecifer)) {
                    $limit = "LIMIT " . $r->length . " OFFSET " . $r->start;
                    $this->limit = $limit;
                    $this->mainQuery = str_replace($limitSpecifier, $limit, $this->mainQuery);
                } else {
                    $this->mainQuery = str_replace($limitSpecifier, $r->length, $this->mainQuery);
                    $this->mainQuery = str_replace($additionalSpecifer, $r->start, $this->mainQuery);
                }
            }
        }
    }

    /**
     * For Simple Query in a single table that doesn't require joining process
     * The simpler version of complex_res()
     * Only support the usage of Laravel Eloquent Model and Query Builder
     *
     * Future abstraction might include Plain Query
     *
     * @param @see complex_res() for full explanation
     */
    public function simple_res(Request $r, $queryBundles, $startColumn, $columns, $useNumbering,
                                $elqModel = null) {
        $primaryKey = array_key_exists('primary_key', $queryBundles) ? $queryBundles['primary_key'] : null;
        $tableName =  array_key_exists('table_name', $queryBundles) ? $queryBundles['table_name'] : null;
        $baseCountQuery = $queryBundles['base_count_query'];
        $baseCountQueryCondSpecifier = $queryBundles['base_count_query_cond_specifier'];
        $nonCaseSensitiveLike = $queryBundles['non_case_sensitive_like_operator'];
        $toStringCaster = [
            'caster' => array_key_exists('to_string_caster', $queryBundles) ?
                $queryBundles['to_string_caster'] : 'CAST(? AS VARCHAR)',
            'specifier' => array_key_exists('to_string_caster_specifier', $queryBundles) ?
                $queryBundles['to_string_caster_specifier'] : '?'
        ];

        if(!empty($elqModel)) {
            $this->mainQuery = $elqModel::where(!empty($primaryKey) ? $primaryKey :
                                (new $elqModel)->getKeyName(), '<>', '0');
            $this->queryType = self::QUERY_TYPE_ELOQUENT;
        } else if(!empty($tableName)) {
            $this->mainQuery = DB::table($tableName);
            $this->queryType = self::QUERY_TYPE_QUERY_BUILDER;
        }

        // set some filters to main query based on request
        $this->query_filter($r, $startColumn, $columns, $nonCaseSensitiveLike, $toStringCaster);
        // set ordering to main query
        $this->query_order($r, $startColumn, $columns);
        // set limitation to main query
        $this->query_limit($r);

        $data = $this->mainQuery->get();
        $formattedData = $this->getFormattedData($data, $startColumn, $columns, $r->columns, $useNumbering, $r->start + 1);
        $recordsTotal = DB::select(str_replace($baseCountQueryCondSpecifier, '', $baseCountQuery))[0]->count;
        $recordsFiltered = DB::select(str_replace($baseCountQueryCondSpecifier, !empty($this->filter) ?
                            $this->filter : '', $baseCountQuery))[0]->count;

        return $this->output($r->draw, $formattedData, $recordsTotal, $recordsFiltered);
    }

    /**
     * For Custom query, It's usually a complex query with many joins involved
     * Any Custom queries can be executed using the given parameters
     * Eloquent Model or Query Builder is not available for this type of function for now (might be added on the next version)
     *
     * @param Request $r                    Data Table client requests
     * @param array $queryBundles           Bundles of properties that are required for the main query, this includes (in each key) :
     *                                      1.   'primary_key' - The primary key - SIMPLE
     *                                      2.   'table_name' - The table name - SIMPLE (OPTIONAL)
     *                                      3.   'query_container' - Base Plain SQL Query - Some Parts will be filled by given specifiers (Condition, Ordering, Limitation, and Additional Specifier) - COMPLEX
     *                                      4.   'base_count_query' - Base Count Plain SQL Query - Only Primary Key counting (SELECT count(PK) ...) - SIMPLE AND COMPLEX
     *                                      5.   'base_count_query_cond_specifier' - Condition Specifier for Base Count Plain SQL Query - SIMPLE AND COMPLEX
     *                                      6.   'non_case_sensitive_like_operator' - Non Case Sensitive 'LIKE' operator for specified SQL engine
     *                                           *note : if this key not specified, default value will be 'LIKE'. But, it's strongly recommend to add value to this.
     *                                      7.   'specifier_cond' - Specifier for custom condition, needed for search query request (will be replaced by some strings) - COMPLEX
     *                                      8.   'specifier_order' - Specifier for custom ordering  (will be replace by some strings) - COMPLEX
     *                                      9.   'specifier_limit' - Specifier for custom limitation  (will be replace by some strings) - COMPLEX
     *                                      10.  'specifier_additional' - Specifier for additional custom dynamic value - COMPLEX
     *                                      11.  'pre_ordered' - A flag denotes if it's a plain query and it's either preordered by certain column or otherwise - COMPLEX
     *                                      12.  'to_string_caster' - A SQL statement for casting any value to String. It requires to search all columns as a string.
     *                                            *note : if this key not specified, default value will be 'CAST(? AS VARCHAR)'.
     *                                      13.  'to_string_caster_specifier' - A column specifier for to_string_caster
     *                                            *note : if this key not specified, default value will be '?'. Make sure that this matches to the specifier included in to_string_specifier. Otherwise, an exception returned.
     *                                      *note : The rightmost word denote that key should be included in Either Complex Query or Simple Query (SIMPLE/COMPLEX)
     * @param mixed $startColumn            Start index of request columns that are included in the query
     * @param array $columns                Array of columns where a single column some properties:
     *                                      1. column name denoted by key value 'col'
     *                                      2. data/column specifier denoted by key value 'dt'
     *                                      3. column alias denoted by key value 'alias', this property is used for customer query.
     *                                         it's common where a query includes multiple table and they are identified by their table aliases.
     *                                         this property is not a must for a single table query where a column con be called without using its table alias.
     *                                         it's a must to give a null value for this property if a column doesn't have a table alias.
     */
    public function complex_res(Request $r, $queryBundles, $startColumn, $columns, $useNumbering) {
        $queryContainer = $queryBundles['query_container'];
        $baseCountQuery = $queryBundles['base_count_query'];
        $baseCountQueryCondSpecifier = $queryBundles['base_count_query_cond_specifier'];
        $nonCaseSensitiveLike = array_key_exists('non_case_sensitive_like_operator', $queryBundles) ?
                                $queryBundles['non_case_sensitive_like_operator'] : 'LIKE';
        $condSpecifier = $queryBundles['specifier_cond'];
        $orderSpecifier = $queryBundles['specifier_order'];
        $limitSpecifier = $queryBundles['specifier_limit'];
        $additionalSpecifier = array_key_exists('specifier_additional', $queryBundles) ?
                            $queryBundles['specifier_additional'] : null;
        $preOrdered = array_key_exists('pre_ordered', $queryBundles) ? $queryBundles['pre_ordered'] : false;
        $toStringCaster = [
            'caster' => array_key_exists('to_string_caster', $queryBundles) ?
                $queryBundles['to_string_caster'] : 'CAST(? AS VARCHAR)',
            'specifier' => array_key_exists('to_string_caster_specifier', $queryBundles) ?
                $queryBundles['to_string_caster_specifier'] : '?'
        ];

        $this->mainQuery = $queryContainer;
        $this->queryType = self::QUERY_TYPE_PLAIN;

        // set some filters to main query based on request
        $this->query_filter($r, $startColumn, $columns, $nonCaseSensitiveLike, $toStringCaster,  $condSpecifier);
        // set ordering to main query
        $this->query_order($r, $startColumn, $columns, $orderSpecifier, $preOrdered);
        // set limitation to main query
        $this->query_limit($r, $limitSpecifier, $additionalSpecifier);

        $data = DB::select($this->mainQuery);
        $formattedData = $this->getFormattedData($data, $startColumn, $columns, $r->columns, $useNumbering, $r->start + 1);
        $recordsTotal = DB::select(str_replace($baseCountQueryCondSpecifier, '', $baseCountQuery))[0]->count;
        $recordsFiltered = DB::select(str_replace($baseCountQueryCondSpecifier, !empty($this->filter) ?
                            $this->filter : '', $baseCountQuery))[0]->count;

        return $this->output($r->draw, $formattedData, $recordsTotal, $recordsFiltered);
    }

    /**
     * Convert original query result to required DataTable output format
     *
     * @param $rawData          Original query result
     * @param $startColumn
     * @param $columns
     * @param $rColumns
     * @return array
     */
    private function getFormattedData($rawData, $startColumn, $columns, $rColumns, $useNumbering = false, $currNumbering = 1) {
        $dtColumns = array_column($columns, 'dt');
        $colNames = [];

        for($i = $startColumn; $i < count($rColumns); $i ++) {
            $colInd = array_search($rColumns[$i]['data'], $dtColumns);
            // push column name to array of column names
            if($colInd !== false)
                array_push($colNames, $columns[$colInd]['col']);
        }

        $res = [];

        foreach ($rawData as $rd) {
            $currData = [];

            if($useNumbering)
                array_push($currData, $currNumbering  ++);

            foreach ($colNames as $col) {
                // save columns data as array stored in $currData
                array_push($currData, $rd->$col);
            }
            // push $currData to $res as the correct format data for DataTable output
            array_push($res, $currData);
        }

        return $res;
    }

    /**
     * Converts data results data to output format (based on Data Table ajax output)
     *
     * @param int $draw                     Sequence of current request
     * @param array $data                   Array of all rows column data
     * @param int $recordsTotal             Total records returned
     * @param int $recordsFiltered          Total returned records filtered
     * @return false|string
     */
    private function output($draw, $data = [], $recordsTotal = 0, $recordsFiltered = 0) {
        $res = new \stdClass();

        $res->draw = $draw;
        $res->recordsTotal = $recordsTotal;
        $res->recordsFiltered = $recordsFiltered;
        $res->data = $data;

        // output directly to json format
        if($this->outputAsJson)
            return json_encode($res);

        return $res;
    }
}