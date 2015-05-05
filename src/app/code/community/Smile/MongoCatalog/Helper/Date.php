<?php
/**
 * Smile mongo catalog helper date
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2014 Smile
 */
class Smile_MongoCatalog_Helper_Date extends Mage_Core_Helper_Abstract
{
    /**
     * Convert date string to ISO Date string
     *
     * @param string $value Date value
     *
     * @return string
     */
    public function getMongoDateFormat($value){
        return new MongoDate(strtotime($value));
    }

    /**
     * Convert mongo date to date/time format
     *
     * @param MongoDate $value
     *
     * @return bool|string
     */
    public function getDateTimeFormat(MongoDate $value){
        return date("Y-m-d H:i:s", $value->sec);
    }
}