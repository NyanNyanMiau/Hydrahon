<?php

namespace ClanCats\Hydrahon\Query\Sql;

/**
 * Select base
 *
 * Implements common filtering functions like: where, limit and offset
 *
 **
 * @package         Hydrahon
 * @copyright       2015 Mario Döring
 */

class SelectBase extends Base
{

	/**
	 * join container
	 *
	 * @var array
	 */
	protected $joins = array();

	/**
	 * order by container
	 *
	 * @var array
	 */
	protected $orders = array();


    /**
     * The query where statements
     *
     * @var array
     */
    protected $wheres = array();

    /**
     * the query offset
     *
     * @var int
     */
    protected $offset = null;

    /**
     * the query limit
     *
     * @var int
     */
    protected $limit = null;


    public function addJoins(Select $preparedSelect)
    {
    	if ( count($preparedSelect->joins) ){
    		$this->joins = array_merge($this->joins, $preparedSelect->joins);
    		$this->flags["aAliasToTable"] = array_merge($this->flags["aAliasToTable"], $preparedSelect->flags["aAliasToTable"]);
    	}
    	return $this;
    }


    /**
     * Add a join statement to the current query
     *
     *     ->join('avatars', 'users.id', '=', 'avatars.user_id')
     *
     * @param array|string              $table The table to join. (can contain an alias definition.)
     * @param string                    $localKey
     * @param string                    $operator The operator (=, !=, <, > etc.)
     * @param string                    $referenceKey
     * @param string                    $type The join type (inner, left, right, outer)
     *
     * @return self The current query builder.
     */
    public function join($table, $localKey, $operator = null, $referenceKey = null, $type = 'left')
    {
    	// validate the join type
    	if (!in_array($type, array('inner', 'left', 'right', 'outer')))
    	{
    		throw new Exception('Invalid join type "'.$type.'" given. Available type: inner, left, right, outer');
    	}

    	$table = trim($table);
    	if(!is_string($table))
    	{
    		throw new Exception('Invalid table type given. Available type: string');
    	}

    	// cbx - copy from base - ignore database "dot"
    	// cbx
    	$tableName = $tableAlias = $table;

    	if (strpos($table, ' as ') !== false)
    	{
    		$tableParts = explode(' as ', $table);
    		$table = array($tableParts[0] => $tableParts[1]);
    		// cbx
    		$tableName = $tableParts[0]; $tableAlias = $tableParts[1];
    	}else
		if (strpos($table, ' ') !== false)
		{
			$tableParts = explode(' ', $table);
			$table = array($tableParts[0] => $tableParts[1]);
			// cbx
			$tableName = $tableParts[0]; $tableAlias = $tableParts[1];
		}
    	// cbx
    	$this->flags["aAliasToTable"][ $tableAlias ] = $tableName;


    	// to make nested joins possible you can pass an closure
    	// wich will create a new query where you can add your nested wheres
    	if (is_object($localKey) && ($localKey instanceof \Closure))
    	{
    		// create new query object
    		$subquery = new SelectJoin;

    		// run the closure callback on the sub query
    		call_user_func_array($localKey, array(&$subquery));

    		// add the join
    		$this->joins[] = array($type, $table, $subquery); return $this;
    	}

    	$this->joins[] = array($type, $table, $localKey, $operator, $referenceKey); return $this;
    }

    /**
     * Left join same as join with special type
     *
     * @param array|string              $table The table to join. (can contain an alias definition.)
     * @param string                    $localKey
     * @param string                    $operator The operator (=, !=, <, > etc.)
     * @param string                    $referenceKey
     *
     * @return self The current query builder.
     */
    public function leftJoin($table, $localKey, $operator = null, $referenceKey = null)
    {
    	return $this->join($table, $localKey, $operator, $referenceKey, 'left');
    }

    /**
     * Alias of the `join` method with join type right.
     *
     * @param array|string              $table The table to join. (can contain an alias definition.)
     * @param string                    $localKey
     * @param string                    $operator The operator (=, !=, <, > etc.)
     * @param string                    $referenceKey
     *
     * @return self The current query builder.
     */
    public function rightJoin($table, $localKey, $operator = null, $referenceKey = null)
    {
    	return $this->join($table, $localKey, $operator, $referenceKey, 'right');
    }

    /**
     * Alias of the `join` method with join type inner.
     *
     * @param array|string              $table The table to join. (can contain an alias definition.)
     * @param string                    $localKey
     * @param string                    $operator The operator (=, !=, <, > etc.)
     * @param string                    $referenceKey
     *
     * @return self The current query builder.
     */
    public function innerJoin($table, $localKey, $operator = null, $referenceKey = null)
    {
    	return $this->join($table, $localKey, $operator, $referenceKey, 'inner');
    }

    /**
     * Alias of the `join` method with join type outer.
     *
     * @param array|string              $table The table to join. (can contain an alias definition.)
     * @param string                    $localKey
     * @param string                    $operator The operator (=, !=, <, > etc.)
     * @param string                    $referenceKey
     *
     * @return self The current query builder.
     */
    public function outerJoin($table, $localKey, $operator = null, $referenceKey = null)
    {
    	return $this->join($table, $localKey, $operator, $referenceKey, 'outer');
    }


    /**
     * Add an order by statement to the current query
     *
     *     ->orderBy('created_at')
     *     ->orderBy('modified_at', 'desc')
     *
     *     // multiple order statements
     *     ->orderBy(['firstname', 'lastname'], 'desc')
     *
     *     // muliple order statements with diffrent directions
     *     ->orderBy(['firstname' => 'asc', 'lastname' => 'desc'])
     *
     * @param array|string              $cols
     * @param string                    $order
     * @return self The current query builder.
     */
    public function orderBy($columns, $direction = 'asc')
    {
    	if (is_string($columns))
    	{
    		$columns = $this->stringArgumentToArray($columns);
    	}
    	elseif ($columns instanceof Expression)
    	{
    		$this->orders[] = array($columns, $direction); return $this;
    	}
    	elseif ($columns instanceof Func)
    	{
    		$this->orders[] = array($columns, $direction); return $this;
    	}

    	foreach ($columns as $key => $column)
    	{
    		if (is_numeric($key))
    		{
    			if ($column instanceof Expression)
    			{
    				$this->orders[] = array($column, $direction);
    			} else {
    				$this->orders[$column] = $direction;
    			}
    		} else {
    			$this->orders[$key] = $column;
    		}
    	}

    	return $this;
    }



    /**
     * Returns an string argument as parsed array if possible
     *
     * @param string                $argument
     * @return array
     */
    protected function stringArgumentToArray($argument)
    {
        if ( strpos($argument, ',') !== false )
        {
            return array_map('trim', explode(',', $argument));
        }

        return array($argument);
    }

    /**
     * Will reset the current orders condition
     *
     * @return self The current query builder.
     */
    public function resetOrders()
    {
    	$this->orders = null; return $this;
    }

    /**
     * Will reset the current selects where conditions
     *
     * @return self The current query builder.
     */
    public function resetWheres()
    {
        $this->wheres = array(); return $this;
    }

    /**
     * Will reset the current selects limit
     *
     * @return self The current query builder.
     */
    public function resetLimit()
    {
        $this->limit = null; return $this;
    }

    /**
     * Will reset the current selects offset
     *
     * @return self The current query builder.
     */
    public function resetOffset()
    {
        $this->offset = null; return $this;
    }

    /**
     * cbx - add wheres from other select statement
     */
    public function addWheres(Select $preparedSelect, $type = "and")
    {
    	$this->where(function($s) use($preparedSelect) {
    		$s->mergeWheres($preparedSelect);
    	}, null, null, $type);
    	return $this;
    }
    public function mergeWheres(Select $preparedSelect)
    {
    	if ( count($preparedSelect->wheres) )
    	{
    		$this->wheres = array_merge($this->wheres, $preparedSelect->wheres);
    	}
    	return $this;
    }


    /**
     * Create a where statement
     *
     *     ->where('name', 'ladina')
     *     ->where('age', '>', 18)
     *     ->where('name', 'in', array('charles', 'john', 'jeffry'))
     *
     * @param string|array      $column The SQL column or an array of column => value pairs.
     * @param mixed             $param1 Operator or value depending if $param2 isset.
     * @param mixed             $param2 The value if $param1 is an opartor.
     * @param string            $type the where type ( and, or )
     *
     * @return self The current query builder.
     */
    public function where($column, $param1 = null, $param2 = null, $type = null)
    {

    	return $this->appendConditional('where', $column, $param1, $param2, $type);
    }

    /**
     * Parse the parameters that make a conditional statement
     *
     * @param string            $statement The type of conditional statement ( where, having )
     * @param string|array      $column The SQL column or an array of column => value pairs.
     * @param mixed             $param1
     * @param mixed             $param2
     * @param string            $type
     */
    protected function appendConditional($statement, $column, $param1 = null, $param2 = null, $type = null)
    {
    	$type = $type ? $type : 'and';
        // check if the type is valid
        if (!in_array($type, array('and', 'or', 'where', 'having')))
        {
            throw new Exception('Invalid condition type "'.$type.'", must be one of the following: and, or, where, having');
        }

        /** @var array $array A reference to the object's property that hold the conditions ($this->wheres, $this->havings) */
        $array = &$this->{$statement . 's'};

        if (empty($array)) {
            $type = $statement;
        } elseif ($type === $statement) {
            $type = 'and';
        }

        // when column is an array, add conditions in bulk
        if (is_array($column))
        {
            $subquery = new static;
            foreach ($column as $key => $val)
            {
                $subquery->appendConditional($statement, $key, $val, null, $type);
            }

            $array[] = array($type, $subquery);
            return $this;
        }

        // to make nested wheres/havings possible you can pass an closure
        // wich will create a new query where you can add your nested wheres/havings
        if (is_object($column) && ($column instanceof \Closure))
        {
            // create new query object
            $subquery = new static;

            // run the closure callback on the sub query
            call_user_func_array($column, array( &$subquery ));

            $array[] = array($type, $subquery);
            return $this;
        }

        // when param2 is null we replace param2 with param one as the
        // value holder and make param1 to the = operator.
        if (is_null($param2))
        {
            $val = $param1; $param1 = '=';
        }else {

        	// if the param2 is an array we filter it. Im no more sure why
        	// but it's there since 4 years so i think i had a reason.
        	// edit: Found it out, when param2 is an array we probably
        	// have an "in" or "between" statement which has no need for duplicates.
        	if (is_array($param2))
        	{
        		$val = array_unique($param2);

        	}
//         	elseif ( is_object($param2) ){
//         		// we expect the opject has a variable "value"
//         		$val = &$param2->value;
//         	}
        	else{
        		$val = $param2;
        	}
        }


        $array[] = array($type, $column, $param1, &$val);
//         $array[] = array($type, $column, $op, &$val);
        return $this;
    }

    /**
     * Create an or where statement
     *
     * This is the same as the normal where just with a fixed type
     *
     * @param string        $column            The SQL column
     * @param mixed        $param1
     * @param mixed        $param2
     *
     * @return self The current query builder.
     */
    public function orWhere($column, $param1 = null, $param2 = null)
    {
        return $this->where($column, $param1, $param2, 'or');
    }

    /**
     * Create an and where statement
     *
     * This is the same as the normal where just with a fixed type
     *
     * @param string        $column            The SQL column
     * @param mixed        $param1
     * @param mixed        $param2
     *
     * @return self The current query builder.
     */
    public function andWhere($column, $param1 = null, $param2 = null)
    {
        return $this->where($column, $param1, $param2, 'and');
    }

    /**
     * Creates a where in statement
     *
     *     ->whereIn('id', [42, 38, 12])
     *
     * @param string                    $column
     * @param array                     $options
     * @return self The current query builder.
     */
    public function whereIn($column, array $options = array())
    {
        // when the options are empty we skip
        if (empty($options))
        {
            return $this;
        }

        return $this->where($column, 'in', $options);
    }

    /**
     * Creates a where not in statement
     *
     *     ->whereIn('id', [42, 38, 12])
     *
     * @param string                    $column
     * @param array                     $options
     * @return self The current query builder.
     */
    public function whereNotIn($column, array $options = array())
    {
        // when the options are empty we skip
        if (empty($options))
        {
            return $this;
        }

        return $this->where($column, 'not in', $options);
    }

    /**
     * Creates a where something is null statement
     *
     *     ->whereNull('modified_at')
     *
     * @param string                    $column
     * @return self The current query builder.
     */
    public function whereNull($column)
    {
        return $this->where($column, 'is', $this->raw('NULL'));
    }

     /**
     * Creates a where something is not null statement
     *
     *     ->whereNotNull('created_at')
     *
     * @param string                    $column
     * @return self The current query builder.
     */
    public function whereNotNull($column)
    {
        return $this->where($column, 'is not', $this->raw('NULL'));
    }

    /**
     * Creates a or where something is null statement
     *
     *     ->orWhereNull('modified_at')
     *
     * @param string                    $column
     * @return self The current query builder.
     */
    public function orWhereNull($column)
    {
        return $this->orWhere($column, 'is', $this->raw('NULL'));
    }

    /**
     * Creates a or where something is not null statement
     *
     *     ->orWhereNotNull('modified_at')
     *
     * @param string                    $column
     * @return self The current query builder.
     */
    public function orWhereNotNull($column)
    {
        return $this->orWhere($column, 'is not', $this->raw('NULL'));
    }


    /**
     * Set the query limit
     *
     *     // limit(<limit>)
     *     ->limit(20)
     *
     *     // limit(<offset>, <limit>)
     *     ->limit(60, 20)
     *
     * @param int           $limit
     * @param int           $limit2
     * @return self The current query builder.
     */
    public function limit($limit, $limit2 = null)
    {
        if (!is_null($limit2))
        {
            $this->offset = (int) $limit;
            $this->limit = (int) $limit2;
        } else {
            $this->limit = (int) $limit;
        }

        return $this;
    }

    /**
     * Set the queries current offset
     *
     * @param int               $offset
     * @return self The current query builder.
     */
    public function offset($offset)
    {
        $this->offset = (int) $offset; return $this;
    }

    /**
     * Create a query limit based on a page and a page size
     *
     * @param int        $page
     * @param int         $size
     * @return self The current query builder.
     */
    public function page($page, $size = 25)
    {
        if (($page = (int) $page) < 0)
        {
            $page = 0;
        }

        $this->limit = (int) $size;
        $this->offset = (int) $size * $page;

        return $this;
    }
}
