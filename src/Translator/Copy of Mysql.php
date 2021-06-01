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
        return $this->attributes[$key];
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
     * - CBX, added using of reference
     *
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

	/*
	 * copy from parseTable
	 * to extract parts
	 *
	 * database.table as alias
	 * add to reference array for used tables
	 * return parts
	 * */
    public function processTableString($passedString = null)
    {
    	$string = $passedString;

    	if ($string == null )
    	{
    		$table = $this->attr('table');
    		$database = $this->attr('database');
    		$alias = null;
    	}else{

    		$table =null;
    		$database = null;
    		$alias = null;
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
    			list($subQuery, $subQueryParameters) = $translator->translate($table[key($table)]);

    			// merge the parameters
    			foreach($subQueryParameters as $parameter)
    			{
    				$this->addParameter($parameter);
    			}

    			// add subquery alias
    			$alias = key($table);
    			$build = '(' . $subQuery . ') as ' . $this->escape($alias);

    			// add alias to for field assigen
    			$this->_aliasToTable[$alias] = $table;

    			// return build and table alias
    			return [$build, $alias, $alias];
    		}

    		// otherwise continue with normal table
    		if ($allowAlias)
    		{
    			$table = key($table) . ' as ' . $table[key($table)];
    		} else {
    			$table = key($table);
    		}
    	}

    	// the string might contain an 'as' statement that we wil have to split.
    	if (strpos($string, ' as ') !== false)
    	{
    		list($string, $alias, $rest) = explode(' as ', $string);
    		$alias = trim($alias);
    		$string = trim($string);

    		if ( $rest!== null ){
    			throw new \Exception('only 2 values expected, table "as" split: '.$passedString);
    		}
    		if ( !strlen($alias) ){
    			throw new \Exception('alias is empty after table "as" split: '.$passedString);
    		}
    		if ( !strlen($string) ){
    			throw new \Exception('table string is empty after table "as" split: '.$passedString);
    		}
    	}

    	// it also might contain dott seperations we have to split
    	// could be columns with dots in name
    	if (strpos($string, '.') !== false)
    	{
    		$string = explode('.', $string);
    		if ( count($string) != 2 ){
    			throw new \Exception('wrong count after table "." split');
    		}
    		$database = $string[0];
    		$table = $string[1];
    	}

    	$table = $table ? $table : $string;
    	$alias = $alias ? $alias : $table;

    	// the escaped query part
    	$build = "";
    	if( $database ){ $build = $this->escapeIdentifier($database) . "."; }
    	$build .=  $this->escapeIdentifier($table);
    	if ( $alias != $table ){ $build .= " as " .  $this->escapeIdentifier($alias); }

    	// add alias to for field assigen
    	$this->_aliasToTable[$alias] = $table;

    	// return build and table alias
    	return [$build, $table, $alias];
    }

    /**
     * Escape / wrap an string for sql
     *
     * @param string|object    $string
     * @param array                   $backpass add type="table, field, alias" array of keys and ref vars ["alias"=> &$alias]
     */
    protected function escape($string, &$backpass=[])
    {
    	$type = isset($backpass["type"]) ? $backpass["type"] : "value";

        if (is_object($string))
        {
            if ($this->isExpression($string))
            {
                return $string->value();
            }
            elseif ($this->isFunction($string))
            {
                return $this->escapeFunction($string);
            }

            elseif ( $this->isStdClass($string) )
            {
            	// we use this until param class injected
            	return $this->param($string->value);
            }

            else
            {
                throw new Exception('Cannot translate object of class: ' . get_class($string));
            }
        }

        // the string might contain an 'as' statement that we wil have to split.
        if (strpos($string, ' as ') !== false)
        {
            $string = explode(' as ', $string);
            $alias = trim($string[1]);
            $backpass["alias"] = $alias;
            return $this->escape(trim($string[0]), $backpass) . ' as ' . $this->escape($alias);
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
            $string =  implode('.', $string);

            return $string;
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

        $wrap = false;
        switch ($function->name()){
        	case 'aes_decrypt':
        		$wrap = 'convert(%s using utf8)';
        	break;
        }

        foreach($arguments as &$argument)
        {
            $argument = $this->escape($argument);
        }

        $f = $buffer . implode(', ', $arguments) . ')';

        if ($wrap){
        	$f = sprintf($wrap, $f);
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
     * @return string
     */
    protected function escapeTable($allowAlias = true, $sParseThis=null, &$returnAlias)
    {
    	if ( $sParseThis !== null )
    	{
    		$table = $sParseThis;
    	}
    	else{
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
                list($subQuery, $subQueryParameters) = $translator->translate($table[key($table)]);

                // merge the parameters
                foreach($subQueryParameters as $parameter)
                {
                    $this->addParameter($parameter);
                }

                // add subquery alias
                $alias = key($table);
                $this->_aliasToTable[ $alias ] = $alias;
                $returnAlias = $alias;
                return '(' . $subQuery . ') as ' . $this->escape($alias);
            }

            // otherwise continue with normal table
            if ($allowAlias)
            {
            	$table = key($table) . ' as ' . $table[key($table)];
            } else {
                $table = key($table);
            }
        }

        $this->processTableString($table, $returnAlias);

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
            $params[$key] = $this->param($param);
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
        $build = 'update ' . $this->escapeTable() . ' set ';

        // add the array values.
        foreach ($this->attr('values') as $key => $value) {
            $build .= $this->escape($key) . ' = ' . $this->param($value) . ', ';
        }

        // cut away the last comma and space
        $build = substr($build, 0, -2);

        // build the where statements
        if ($wheres = $this->attr('wheres'))
        {
            $build .= $this->translateWhere($wheres);
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
        $build = 'delete from ' . $this->escapeTable(false);

        // build the where statements
        if ($wheres = $this->attr('wheres'))
        {
            $build .= $this->translateWhere($wheres);
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

    private $aliasTableRef;

    /**
     * Translate the current query to an SQL select statement
     *
     * @return string
     */
    protected function translateSelect()
    {
    	var_export( $this->attr("flags")["aAliasToTable"] );
    	die("----");
        // normal or distinct selection?
        $build = ($this->attr('distinct') ? 'select distinct' : 'select') . ' ';

        // parse table and joins to get used tables and aliases
        list($sTable, $localTable, $localAlias) = $this->processTableString();

        // build the join statements
        if ($this->attr('joins'))
        {
        	$sJoins = $this->translateJoins();
        }

        // build the selected fields
        $fields = $this->attr('fields');

        if (!empty($fields))
        {
            foreach ($fields as $key => $field)
            {
                list($column, $alias) = $field;

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
        $build .= ' from ' . $sTable;

        // append joins
        $build .= $sJoins;


        var_Export( $build);

        var_Export( $this->_aliasToTable );
        die();



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
    	die();
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

                // The parameters get added by the call of compile where
                $build .= ' ' . $condition[0] . ' ( ' . $translatedSubConditions . ' )';

                continue;
            }

            // when we have an array as where values we have
            // to parameterize them
            if (is_array($condition[3]))
            {
                $condition[3] = '(' . $this->parameterize($condition[3]) . ')';
            } else {
                $condition[3] = $this->param($condition[3]);
            }

            // we always need to escape the key
            $condition[1] = $this->escape($condition[1]);

            // implode the beauty
            $build .= ' ' . implode(' ', $condition);
        }

        return $build;
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
            list($sJoinTable, $refTable, $refAlias) = $this->processTableString($table);

            $build .= $sJoinTable;


//             if (is_array($table))
//             {
//                 reset($table);

//                 // the table might be a subselect so check that
//                 // first and compile the select if it is one
//                 if ($table[key($table)] instanceof Select)
//                 {
//                     $translator = new static;

//                     // translate the subselect
//                     list($subQuery, $subQueryParameters) = $translator->translate($table[key($table)]);

//                     // merge the parameters
//                     foreach($subQueryParameters as $parameter)
//                     {
//                         $this->addParameter($parameter);
//                     }

//                     // add subquery alias
//                     $alias = key($table);
//                     $this->_aliasToTable[ $alias ] = $alias;
//                     $build .= '(' . $subQuery . ') as ' . $this->escape($alias);
//                 }else{
//                 	// add code here?
//                 }
//             } else {
//                 // start the join
//             	$build .= $this->escape($table);
//             }

            // start the join
            $build .= ' on ( ';


            // to make nested join conditions possible you can pass an closure
            // wich will create a new query where you can add your nested ons and wheres
            if (!isset($join[3]) && isset($join[2]) && $join[2] instanceof BaseQuery)
            {
                $subAttributes = $join[2]->attributes();

                $joinConditions = '';

                // remove the first type from the ons
                reset($subAttributes['ons']);
                $subAttributes['ons'][key($subAttributes['ons'])][0] = '';

                foreach($subAttributes['ons'] as $on)
                {
                    list($type, $localKey, $operator, $referenceKey) = $on;
                    $joinConditions .= ' ' . $type . ' ' . $this->escape($localKey) . ' ' . $operator . ' ' . $this->escape($referenceKey);
                }

                $build .= trim($joinConditions);

                // compile the where if set
                if (!empty($subAttributes['wheres']))
                {
                    $build .= ' and ' . substr($this->translateWhere($subAttributes['wheres']), 7);
                }
            }
            else
            {
                // othewise default join
                list($type, $table, $localKey, $operator, $referenceKey) = $join;
                $build .= $this->escape($localKey) . ' ' . $operator . ' ' . $this->escape($referenceKey);
            }

            $build .= ' ) ';
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
                //list($column, $direction) = $direction;
                $column = $direction[0];
                $direction = $direction[1];
            }

            $build .= $this->escape($column) . ' ' . $direction . ', ';
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
        list($subQuery, $subQueryParameters) = $translator->translate($this->attr('select'));

        // merge the parameters
        foreach($subQueryParameters as $parameter)
        {
            $this->addParameter($parameter);
        }

        return 'select exists(' . $subQuery .') as `exists`';
    }
}
