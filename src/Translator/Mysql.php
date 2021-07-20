<?php namespace ClanCats\Hydrahon\Translator;

/**
 * Mysql query translator
 **
 * @package         Hydrahon
 * @copyright       2015 Mario Döring
 */

use ClanCats\Hydrahon\BaseQuery;
use ClanCats\Hydrahon\Query\Expression;
use ClanCats\Hydrahon\TranslatorInterface;
use ClanCats\Hydrahon\Exception;

use ClanCats\Hydrahon\Query\Sql\Select;
use ClanCats\Hydrahon\Query\Sql\Insert;
use ClanCats\Hydrahon\Query\Sql\Replace;
use ClanCats\Hydrahon\Query\Sql\Update;
use ClanCats\Hydrahon\Query\Sql\Delete;
use ClanCats\Hydrahon\Query\Sql\Drop;
use ClanCats\Hydrahon\Query\Sql\Truncate;
use ClanCats\Hydrahon\Query\Sql\Func;
use ClanCats\Hydrahon\Query\Sql\Exists;

use ClanCats\Hydrahon\Query\Sql\Show;

class Mysql implements TranslatorInterface
{
    /**
     * The query parameters
     *
     * @var array
     */
    protected $parameters = array();

    /**
     * The current query attributes
     *
     * @param array
     */
    protected $attributes = array();

    /**
     * Translate the given query object and return the results as
     * argument array
     *
     * @param ClanCats\Hydrahon\BaseQuery                 $query
     * @return array
     */
    public function translate(BaseQuery $query)
    {
        // retrive the query attributes
        $this->attributes = $query->attributes();

        // handle SQL SELECT queries
        if ($query instanceof Select)
        {
            $queryString = $this->translateSelect();
        }
        // handle SQL INSERT queries
        elseif ($query instanceof Replace)
        {
            $queryString = $this->translateInsert('replace');
        }
        // handle SQL INSERT queries
        elseif ($query instanceof Insert)
        {
            $queryString = $this->translateInsert('insert');
        }
        // handle SQL UPDATE queries
        elseif ($query instanceof Update)
        {
            $queryString = $this->translateUpdate();
        }
        // handle SQL UPDATE queries
        elseif ($query instanceof Delete)
        {
            $queryString = $this->translateDelete();
        }
        // handle SQL DROP queries
        elseif ($query instanceof Drop)
        {
            $queryString = $this->translateDrop();
        }
        // handle SQL TRUNCATE queries
        elseif ($query instanceof Truncate)
        {
            $queryString = $this->translateTruncate();
        }
        elseif ($query instanceof Exists)
        {
            $queryString = $this->translateExists();
        }
        elseif ($query instanceof Show)
        {
        	$queryString = $this->translateShow();
        }
        // everything else is wrong
        else
        {
            throw new Exception('Unknown query type. Cannot translate: '.get_class($query));
        }

        // get the query parameters and reset
        $queryParameters = $this->parameters; $this->clearParameters();

        return array($queryString, $queryParameters);
    }

    /**
     * Returns the an attribute value for the given key
     *
     * @param string                $key
     * @return mixed
     */
    protected function attr($key)
    {
    	return isset($this->attributes[$key]) ? $this->attributes[$key] : null;
    }

    /**
     * Check if the given argument is an sql expression
     *
     * @param mixed                 $expression
     * @return bool
     */
    protected function isExpression($expression)
    {
        return $expression instanceof Expression;
    }

    /**
     * Check if the given argument is an sql function
     *
     * @param mixed                 $expression
     * @return bool
     */
    protected function isFunction($function)
    {
        return $function instanceof Func;
    }

    protected function isSelect($select)
    {
    	return $select instanceof Select;
    }


    protected function isStdClass($param){
    	return $param instanceof \stdClass;
    }

    /**
     * Clear all set parameters
     *
     * @return void
     */
    protected function clearParameters()
    {
        $this->parameters = array();
    }

    /**
     * Adds a parameter to the builder
     *
     * - CBX - added using of reference
     * - @todo allow to pass parameter type
     * @return void
     */
    protected function addParameter(&$value)
    {
        $this->parameters[] = $value;
    }

    /**
     * creates an parameter and adds it
     *
     * @param mixed         $value
     * @return string
     */
    protected function param(&$value)
    {
        if (!$this->isExpression($value))
        {
        	if ( is_array($value) ){
        		$value = reset($value);
        	}
        	if ( strtolower($value) === "null"){ return 'null'; } // conditions with "field is ?" - lead to syntax error if "null" is added per "?" = "null"
            $this->addParameter($value); return '?';
        }

        return $value;
    }

    /**
     * Filters the parameters removes the keys and Expressions
     *
     * @param array         $parameters
     * @return array
     */
    protected function filterParameters($parameters)
    {
        return array_values(array_filter($parameters, function ($item)
        {
            return !$this->isExpression($item);
        }));
    }

    function escapeSelect($select)
    {
    	$translator = new static;

    	// translate the subselect
    	@list($subQuery, $subQueryParameters) = $translator->translate( $select );

    	// merge the parameters
    	foreach($subQueryParameters as $parameter)
    	{
    		$this->addParameter($parameter);
    	}

    	return $subQuery;
    }


    /**
     * Escape / wrap an string for sql
     *
     * @param string|object    $string
     * @param array                   $backpass add type="table, field, alias" array of keys and ref vars ["alias"=> &$alias]
     */
    protected function escape($string)
    {
        if (is_object($string))
        {
            if ($this->isExpression($string))
            {
            	$aes_key = "__AES_KEY__";
            	// @todo replace use of __AES_KEY__ with ? and add to parameters
            	$aParts = explode($aes_key, $string->value());
            	if ( count($aParts) > 1 ){
            		for ($i=0; $i< count($aParts); $i++){
            			$this->addParameter($aes_key);
            		}
            		return implode("?", $aParts);
            	}
                return $string->value();
            }
            elseif ($this->isFunction($string))
            {
                return $this->escapeFunction($string);
            }
            elseif ($this->isSelect($string))
            {
            	return "(" . $this->escapeSelect($string) . ")";
            }

            elseif ( $this->isStdClass($string) )
            {
            	// cbx - we use this until param class injected
            	return $this->param($string->value);
            }

            else
            {
                throw new Exception('Cannot translate object of class: ' . get_class($string));
            }
        }

        // if distinct used with field count(distinct field), prevent escape
        if (strpos($string, 'distinct ') === 0)
        {
        	$string = explode('distinct ', $string);
        	return 'distinct ' . $this->escape(trim($string[1]));
        }

        // the string might contain an 'as' statement that we wil have to split.
        if (strpos($string, ' as ') !== false)
        {
            $string = explode(' as ', $string);

            return $this->escape(trim($string[0])) . ' as ' . $this->escape(trim($string[1]));
        }

        // it also might contain dott seperations we have to split
        // could be columns with dots in name
        if (strpos($string, '.') !== false)
        {
            $string = explode('.', $string);
            foreach ($string as $key => $item)
            {
            	$string[$key] = $this->escapeIdentifier($item);
            }

            return implode('.', $string);
        }

        return $this->escapeIdentifier($string);
    }

    /**
     * Function to escape identifier names (columns and tables)
     * Doubles backticks, removes null bytes
     * https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     *
     * @var string
     */
    public function escapeIdentifier($identifier)
    {
        return '`' . str_replace(array('`', "\0"), array('``',''), $identifier) . '`';
    }

    /**
     * Escapes an sql function object
     *
     * @param Func              $function
     * @return string
     */
    protected function escapeFunction($function)
    {
        $buffer = $function->name() . '(';

        $arguments = $function->arguments();

        /*
         * cbx - added custom behaviour of functions,
         * - test passed table fields are encrypted
         * */
        switch ($function->name())
        {
        	case 'aes_decrypt':
        		$wrap = 'convert(%s using utf8)';

        		@List(&$field, $aes_key) = $arguments;
        		$aes_key = '__AES_KEY__';
        		$f = $buffer . $this->escape($field) . ", " . $this->param($aes_key) . ")";
        		$f = sprintf($wrap, $f);
        	break;


        	/*
        	 * group_concat( distinct field1` order by `field2` seperator ',' )
        	 * note: max return length of group_concat is default 1024 chars
        	 *
        	 * @note dont use group_concat to stick uids
        	 */
        	case 'group_concat':

        		if (count($arguments) > 2) {
        			throw new Exception("only 2 parameters expected, use param2 as keyed array, options avail [distinct, orderby, separator]");
        		}

        		@list(&$field, $options) = $arguments;
        		$distinct  = isset($options["distinct"]) 	? $options["distinct"] : false;
        		$orderby   = isset($options["orderby"]) 	? @list($sort, $dir) = explode(" ", $options["orderby"]) : false;
        		$separator = isset($options["separator"]) 	? $options["separator"] : false;

        		$f = 	( $distinct ? " distinct " : "" )
        				. (
        						is_string($field) && $this->cbx_isFieldEncrypted($field)
        						? $this->escape(new Func('aes_decrypt', $field))
        						: $this->escape($field)
        				)
        				. ( $orderby ? " " . $this->escape($sort) . " " . ($dir?$dir:"asc") : "")
        				. ( $separator? " separator '" . $separator. "'" : "");

	       		$f = $buffer .$f. ")";
			break;

        	case 'str_to_date':
        	case 'from_unixtime':

        		@List(&$field, $datestring) = $arguments;
        		$field = is_string($field) && $this->cbx_isFieldEncrypted($field)
        				 ? $this->escape(new Func('aes_decrypt', $field))
						 : $this->escape($field);

        		$f = $buffer . $field . ', "' . $datestring . '")';
        		break;

        	// convert a field to a number - mysql stuff don't work as expected
        	case '1*':

        		$field = $arguments[0];
				$f = is_string($field) && $this->cbx_isFieldEncrypted($field)
						 ? $this->escape(new Func('aes_decrypt', $field))
						 : $this->escape($field);

				$f = $buffer . $f . ')';
        		break;

        	case 'cast':
        		$field = $arguments[0];
        		$f = is_string($field) && $this->cbx_isFieldEncrypted($field)
		        		? $this->escape(new Func('aes_decrypt', $field))
		        		: $this->escape($field);

		        $f = $buffer . $f . ' as signed)';
        		break;

        	// just a try, else we need to add all functions, to decide where is the field
        	default:

        		foreach($arguments as &$argument){
        			$argument = is_string($argument) && $this->cbx_isFieldEncrypted($argument)
        						 ? $this->escape(new Func('aes_decrypt', $argument))
								 : $this->escape($argument);
//         			$argument = $this->escape($argument);
        		}
        		$f = $buffer . implode(', ', $arguments) . ')';
        	break;
        }

        return $f;
    }

    /**
     * Escape an array of items an seprate them with a comma
     *
     * @param array         $array
     * @return string
     */
    protected function escapeList($array)
    {
        foreach ($array as $key => $item)
        {
            $array[$key] = $this->escape($item);
        }

        return implode(', ', $array);
    }

    /**
     * get and escape the table name
     *
     * @return string
     */
    protected function escapeTable($allowAlias = true, $table=null)
    {
    	$database = null;
    	if ( is_null($table) )
    	{
	        $table = $this->attr('table');
	        $database = $this->attr('database');
    	}
        $buffer = '';

        if (!is_null($database))
        {
            $buffer .= $this->escape($database) . '.';
        }

        // when the table is an array we have a table with alias
        if (is_array($table))
        {
            reset($table);

            // the table might be a subselect so check that
            // first and compile the select if it is one
            if ($table[key($table)] instanceof Select)
            {
                $translator = new static;

                // translate the subselect
                @list($subQuery, $subQueryParameters) = $translator->translate($table[key($table)]);

                // merge the parameters
                foreach($subQueryParameters as $parameter)
                {
                    $this->addParameter($parameter);
                }

                return '(' . $subQuery . ') as ' . $this->escape(key($table));
            }

            // otherwise continue with normal table
            if ($allowAlias)
            {
            	$table = key($table) . ' as ' . $table[key($table)];
            } else {
                $table = key($table);
            }
        }

        return $buffer . $this->escape($table);
    }

    /**
     * Convert data to parameters and bind them to the query
     *
     * @param array         $params
     * @return string
     */
    protected function parameterize($params)
    {
        foreach ($params as $key => $param)
        {
        	if ( is_string($key) && $this->cbx_isFieldEncrypted($key) )
			{
				$aes_key = "__AES_KEY__";
				$params[$key] = 'aes_encrypt(' . $this->param($param) . ', ' . $this->param($aes_key) . ')';
			}
			elseif ($this->isSelect($param))
			{
				$params[$key] = "(" . $this->escapeSelect($param) . ")";
			}

			else{
				$params[$key] = $this->param($param);
			}
        }

        return implode(', ', $params);
    }

    /*
     * -- FROM HERE TRANSLATE FUNCTIONS FOLLOW
     */

    /**
     * Translate the current query to an SQL insert statement
     *
     * @return string
     */
    protected function translateInsert($key)
    {
        $build = ($this->attr('ignore') ? $key . ' ignore' : $key);

        $build .= ' into ' . $this->escapeTable(false) . ' ';

        if (!$valueCollection = $this->attr('values'))
        {
            throw new Exception('Cannot build insert query without values.');
        }

        // Get the array keys from the first array in the collection.
        // We use them as insert keys. If you pass an array collection with
        // missing keys or a diffrent structure well... f*ck
        $build .= '(' . $this->escapeList(array_keys(reset($valueCollection))) . ') values ';

        // add the array values.
        foreach ($valueCollection as $values)
        {
            $build .= '(' . $this->parameterize($values) . '), ';
        }

        // cut the last comma away
        return substr($build, 0, -2);
    }

    /**
     * Translate the current query to an SQL update statement
     *
     * @return string
     */
    protected function translateUpdate()
    {
        $build = 'update ' . $this->escapeTable();

        // build the join statements
        if ($this->attr('joins'))
        {
        	$build .= $this->translateJoins();
        }

        $build .= ' set ';

        // add the array values.
        foreach ($this->attr('values') as $key => $value)
        {
        	$build .= $this->escape($key) . ' = ';

        	if ( $this->cbx_isFieldEncrypted($key) )
        	{
        		$aes_key = "__AES_KEY__";
        		$build .= 'aes_encrypt(' . $this->param($value) . ', ' . $this->param($aes_key) . ')';
        	}else{

        		if ( is_array($value) ){
        			trigger_error("Array passed as value: serialize before! ". $build, E_USER_ERROR);
        		}

        		$build .= $this->param($value);
        	}
        	$build .= ', ';
        }

        // cut away the last comma and space
        $build = substr($build, 0, -2);

        // build the where statements
        if ($wheres = $this->attr('wheres'))
        {
            $build .= $this->translateWhere($wheres);
        }

        // build the order statement
        if ($this->attr('orders'))
        {
        	$build .= $this->translateOrderBy();
        }


        // build offset and limit
        if ($this->attr('limit'))
        {
             $build .= $this->translateLimit();
        }

        return $build;
    }

    /**
     * Translate the current query to an SQL delete statement
     *
     * @return string
     */
    protected function translateDelete()
    {
        $build = 'delete ';

        // build the join statements
        if ($this->attr('delete'))
        {
        	$build .= $this->escapeList(explode(",", $this->attr('delete')));
        }

        $build .= ' from ' . $this->escapeTable(false);

        // build the join statements
        if ($this->attr('joins'))
        {
        	$build .= $this->translateJoins();
        }

        // build the where statements
        if ($wheres = $this->attr('wheres'))
        {
            $build .= $this->translateWhere($wheres);
        }

        // build the order statement
        if ($this->attr('orders'))
        {
        	$build .= $this->translateOrderBy();
        }

        // build offset and limit
        if ($this->attr('limit'))
        {
             $build .= $this->translateLimit();
        }

        return $build;
    }


    public function getDecryptionWrapper()
    {

    }

    public function getEncryptionWrapper()
    {

    }

    // cbx - check if field shall decrypted / encrypted
    function cbx_isFieldEncrypted($field)
    {
    	global $bEncryptionActive, $aEncryptionConfig;

    	if (!is_string($field)){
    		throw new \Exception("Field should be a string:".var_export($field, 1));
    	}

    	// silly coders could have put dots in fieldnames
    	if ( strpos($field, ".") ){
    		@list($table, $field) = explode(".", $field);
    	}else{
    		$table = $this->attr("table");
    		if ( is_array($table) ){
    			$table = key($table);
    		}
    	}
    	// check if its an alias and return db table name
    	if ( isset($this->attr("flags")["aAliasToTable"][$table]) ){
    		$table = $this->attr("flags")["aAliasToTable"][$table];
    	}

    	return  $bEncryptionActive
    			&& isset($aEncryptionConfig[$table])
    			&& in_array($field, (array)$aEncryptionConfig[$table]);

    }

    /**
     * Translate the current query to an SQL select statement
     *
     * @return string
     */
    protected function translateSelect()
    {
        // normal or distinct selection?
        $build = ($this->attr('distinct') ? 'select distinct' : 'select') . ' ';

        // build the selected fields
        $fields = $this->attr('fields');

        if (!empty($fields))
        {
            foreach ($fields as $key => $field)
            {
                @list($column, $alias) = $field;

                // cbx - we need the field alone, "as" should always removed before - see addField
                if ( is_string($column) )
                {
                	if ( $this->cbx_isFieldEncrypted($column) )
                	{
                		if (!$alias){
                			$alias = explode(".",$column);
                			$alias = end($alias);
                		}
                		$column = new Func('aes_decrypt', $column);
                	}
                }

                if (!is_null($alias))
                {
                    $build .= $this->escape($column) . ' as ' . $this->escape($alias);
                }
                else
                {
                    $build .= $this->escape($column);
                }

                $build .= ', ';
            }

            $build = substr($build, 0, -2);
        }
        else
        {
            $build .= '*';
        }

        // append the table
        $build .= ' from ' . $this->escapeTable();

        // build the join statements
        if ($this->attr('joins'))
        {
        	$build .= $this->translateJoins();
        }

        // build the where statements
        if ($wheres = $this->attr('wheres'))
        {
            $build .= $this->translateWhere($wheres);
        }

        // build the groups
        if ($this->attr('groups'))
        {
            $build .= $this->translateGroupBy();
        }

        // build the having statements
        if ($havings = $this->attr('havings'))
        {
            $build .= $this->translateHaving($havings);
        }

        // build the order statement
        if ($this->attr('orders'))
        {
            $build .= $this->translateOrderBy();
        }

        // build offset and limit
        if ($this->attr('limit') || $this->attr('offset'))
        {
            $build .= $this->translateLimitWithOffset();
        }

        return $build;
    }

    /**
     * Translate the where statement into sql
     *
     * @param array                 $wheres
     * @return string
     */
    protected function translateWhere(array $wheres)
    {
        return $this->translateConditional('where', $wheres);
    }

    /**
     * Translate the having statement into sql
     *
     * @param array                 $havings
     * @return string
     */
    protected function translateHaving(array $havings)
    {
        return $this->translateConditional('having', $havings);
    }


    protected function translateFieldDecryption($field)
    {
    	print_r($this);
    	throw new Exception(__method__.' - not done yet');
    }

    /**
     * Translate the conditional statements (where, having) into sql
     *
     * @param string                $statement The name of the statement ( where, having )
     * @param array                 $conditions
     * @return string
     */
    protected function translateConditional($statement, $conditions)
    {
        $build = '';

        foreach ($conditions as $condition)
        {
            // to make nested wheres possible you can pass an closure
            // wich will create a new query where you can add your nested wheres
            if (!isset($condition[2]) && isset( $condition[1] ) && $condition[1] instanceof BaseQuery )
            {
                /** @var array $subConditions The array of $conditions inside the nested query */
                $subConditions = $condition[1]->attributes()[$statement . 's'];

                $translatedSubConditions = $this->translateConditional($statement, $subConditions);

                // remove the statement from the result (+2 for the space before and after)
                $translatedSubConditions = substr($translatedSubConditions, strlen($statement) + 2);

                if (count($subConditions)>1)
                {
	                // The parameters get added by the call of compile where
	                $build .= ' ' . $condition[0] . ' ( ' . $translatedSubConditions . ' )';
                }else{
                	$build .= ' ' . $condition[0] . ' ' . $translatedSubConditions . '';
                }
                continue;
            }

            /*Array (
			    [0] => where
			    [1] => name
			    [2] => =
			    [3] => system/fileversion
			)*/
            $condition_0 = $condition[0];

            // cbx - we need the field alone, "as" remove in addField
            $column = $condition[1];

            $isNumber = is_int($condition[3]) || is_float($condition[3]);

            if ( is_string($column) )
            {
            	// cbx - we need to have a tableprefix if its not an alias
            	if ( !$this->isAlias($column) )
            	{
            		if ( strpos($column, ".") === false ){
            			$column = $this->attr("fieldTablePrefix") . "." . $column;
            		}

            		if ( $this->cbx_isFieldEncrypted($column) )
            		{
            			$column = new Func('aes_decrypt', $column); // , (object)["value"=>"__AES_KEY__"]
            		//             		$alias = !is_null($alias) ? $alias : $testField;
            		}
            	}
            }

            // we always need to escape the key
            $condition_1 = $this->escape($column);

            $condition_2 = $condition[2];

            // when we have an array as where values we have
            // to parameterize them
            if (is_array($condition[3]))
            {
            	if ( $condition_2 === "between" ){
            		@List($v1, $v2) = $condition[3];
            		$condition_3 = $this->param($v1) . ' and ' .$this->param($v2);
            	}else{
                	$condition_3 = '(' . $this->parameterize($condition[3]) . ')';
            	}

            } else {

            	if ( is_object($condition[3]) ){
            		$condition_3 = $this->escape($condition[3]);
            	}else{
                	$condition_3 = $this->param($condition[3]);
            	}

            }

            // implode the beauty
            $build .= " $condition_0 $condition_1 $condition_2 $condition_3";
        }

        return $build;
    }

	function isAlias($column)
	{
		foreach( (array)$this->attr("fields") as $field ){
			if (isset($field[1]) && $field[1] == $column) return true;
		}
		return false;
	}

    function translateOns($attributes)
    {
    	$joinConditions = '';

    	// remove the first type from the ons
    	reset($attributes['ons']);
    	$attributes['ons'][key($attributes['ons'])][0] = '';

    	foreach($attributes['ons'] as $on)
    	{
    		// cbx - on closure
    		if (isset( $on[1] ) && $on[1] instanceof BaseQuery)
    		{
    			/** @var array $subConditions The array of $conditions inside the nested query */
    			$subAttributes = $on[1]->attributes();

    			$translatedSubConditions = $this->translateOns($subAttributes);

    			// remove the statement from the result (+2 for the space before and after)
//     			$translatedSubConditions = substr($translatedSubConditions, strlen($statement) + 2);

    			// The parameters get added by the call of compile where
    			$joinConditions .= ' ' . $on[0] . ' ( ' . $translatedSubConditions . ' )';
    			continue;
    		}

    		@list($type, $localKey, $operator, $referenceKey) = $on;
    		$joinConditions .= ' ' . $type . ' ' . $this->escape($localKey) . ' ' . $operator . ' ' . $this->escape($referenceKey);
    	}

    	$joinConditions = trim($joinConditions);

    	// compile the where if set
    	if (!empty($attributes['wheres']))
    	{
    		$joinConditions .= ' and ' . substr($this->translateWhere($attributes['wheres']), 7);
    	}

    	return $joinConditions;
    }
    /**
     * Build the sql join statements
     *
     * @return string
     */
    protected function translateJoins()
    {
        $build = '';

        foreach ($this->attr('joins') as $join)
        {
            // get the type and table
            $type = $join[0]; $table = $join[1];

            // start the join
            $build .= ' ' . $type . ' join ';

            // table
            $build .= $this->escapeTable(true, $table);

            // start the join
            $build .= ' on ( ';

            // to make nested join conditions possible you can pass an closure
            // wich will create a new query where you can add your nested ons and wheres
            if (!isset($join[3]) && isset($join[2]) && $join[2] instanceof BaseQuery)
            {
                $subAttributes = $join[2]->attributes();

//                 $joinConditions = '';
                $build .= $this->translateOns($subAttributes);

//                 print_r($build);
//                 die();
//                 foreach($subAttributes['ons'] as $on)
//                 {
//                 	var_dump( $on );
//                 	// cbx - on closure
//                 	if (isset( $on[1] ) && $on[1] instanceof SelectJoin)
//                 	{

//                 		var_dump( $on );
//                 		die();

//                 		/** @var array $subConditions The array of $conditions inside the nested query */
//                 		$subConditions = $condition[1]->attributes()[$statement . 's'];

//                 		$translatedSubConditions = $this->translateConditional($statement, $subConditions);

//                 		// remove the statement from the result (+2 for the space before and after)
//                 		$translatedSubConditions = substr($translatedSubConditions, strlen($statement) + 2);

//                 		if (count($subConditions)>1)
//                 		{
//                 			// The parameters get added by the call of compile where
//                 			$joinConditions .= ' ' . $on[0] . ' ( ' . $translatedSubConditions . ' )';
//                 		}else{
//                 			$joinConditions .= ' ' . $on[0] . ' ' . $translatedSubConditions . '';
//                 		}
//                 		continue;
//                 	}

//                 	@list($type, $localKey, $operator, $referenceKey) = $on;
//                 	$joinConditions .= ' ' . $type . ' ' . $this->escape($localKey) . ' ' . $operator . ' ' . $this->escape($referenceKey);
//                 }

//                 $build .= trim($joinConditions);

//                 // compile the where if set
//                 if (!empty($subAttributes['wheres']))
//                 {
//                     $build .= ' and ' . substr($this->translateWhere($subAttributes['wheres']), 7);
//                 }
            }
            else
            {
                // othewise default join
                @list($type, $table, $localKey, $operator, $referenceKey) = $join;
                $build .= $this->escape($localKey) . ' ' . $operator . ' ' . $this->escape($referenceKey);
            }
            $build .= ' )';
        }

        return $build;
    }

    /**
     * Build the order by statement
     *
     * @return string
     */
    protected function translateOrderBy()
    {
        $build = " order by ";

        foreach ($this->attr('orders') as $column => $direction)
        {
            // in case a raw value is given we had to
            // put the column / raw value an direction inside another
            // array because we cannot make objects to array keys.
            if (is_array($direction))
            {
                // This only works in php 7 the php 5 fix is below
                //@list($column, $direction) = $direction;
                $column = $direction[0];
                $direction = $direction[1];
            }


            // cbx - we need to have a tableprefix
            if ( strpos($column, ".") === false ){
            	$column = $this->attr("fieldTablePrefix") . "." . $column;
            }

            if ( is_string($column) && $this->cbx_isFieldEncrypted($column) )
            {
            	$column = new Func('aes_decrypt', $column); // (object)["value"=>"__AES_KEY__"]

            	$column = $this->escape($column) . ' collate utf8_general_ci';
            }else{
            	$column = $this->escape($column);
            }

            $build .= $column. ' ' . $direction . ', ';
        }

        return substr($build, 0, -2);
    }

    /**
     * Build the gorup by statemnet
     *
     * @return string
     */
    protected function translateGroupBy()
    {
        return ' group by ' . $this->escapeList($this->attr('groups'));
    }

    /**
     * Build the limit and offset part
     *
     * @param Query         $query
     * @return string
     */
    protected function translateLimitWithOffset()
    {
        return ' limit ' . ((int) ($this->attr('offset'))) . ', ' . ((int) ($this->attr('limit')));
    }

    /**
     * Build the limit and offset part
     *
     * @param Query         $query
     * @return string
     */
    protected function translateLimit()
    {
        return ' limit ' . ((int) $this->attr('limit'));
    }

    /**
     * Translate the current query to an sql DROP statement
     *
     * @return string
     */
    protected function translateDrop()
    {
        return 'drop table ' . $this->escapeTable() .';';
    }

    /**
     * Translate the current query to an sql DROP statement
     *
     * @return string
     */
    protected function translateTruncate()
    {
        return 'truncate table ' . $this->escapeTable() .';';
    }

    /**
     * Translate the current query to an sql SHOW statement
     *
     * @return string
     */
    protected function translateShow()
    {
    	return 'show columns from ' . $this->escapeTable() .';';
    }



    /**
     * Translate the exists querry
     *
     * @return string
     */
    protected function translateExists()
    {
        $translator = new static;

        // translate the subselect
        @list($subQuery, $subQueryParameters) = $translator->translate($this->attr('select'));

        // merge the parameters
        foreach($subQueryParameters as $parameter)
        {
            $this->addParameter($parameter);
        }

        return 'select exists(' . $subQuery .') as `exists`';
    }
}
