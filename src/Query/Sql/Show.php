<?php namespace ClanCats\Hydrahon\Query\Sql;

/**
 * SQL query object
 **
 * @package         Hydrahon
 * @copyright       2015 Mario Döring
 */

use ClanCats\Hydrahon\BaseQuery;

class Show extends Base implements FetchableInterface
{
    // also here we need the class only to identify the query
	/**
	 * Executes the `executeResultFetcher` callback and handles the results.
	 *
	 * @return mixed The fetched result.
	 */
	public function get()
	{
		// run the callbacks to retirve the results
		$results = $this->executeResultFetcher();

		// we always exprect an array here!
		if (!is_array($results) || empty($results))
		{
			$results = array();
		}

		return $results;
	}

}
