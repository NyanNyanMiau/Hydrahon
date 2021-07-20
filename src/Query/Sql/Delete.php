<?php namespace ClanCats\Hydrahon\Query\Sql;

/**
 * SQL query object
 **
 * @package         Hydrahon
 * @copyright       2015 Mario Döring
 */

class Delete extends SelectBase
{
    // currently delete does nothing sepecial so go on do other stuff..
    protected $delete = null;

    /**
     * set the tables or aliases to delete from
     * if join or multiple table are used
     *
     * @param String $delete
     */
    public function setDelete($delete){
    	$this->delete = $delete;
    	return $this;
    }
}
