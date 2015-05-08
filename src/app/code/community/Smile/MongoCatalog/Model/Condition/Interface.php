<?php
/**
 * Smile MongoCatalog condition object interface
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2015 Smile
 */
interface Smile_MongoCatalog_Model_Condition_Interface
{
    /**
     * Create and/or init an instance of the model
     *
     * @param $attributeData
     * @param $conditionData
     *
     */
    public function initCondition($attributeData, $conditionData);

    /**
     * Return condition type
     *
     * @return string
     */
    public function getConditionType();

    /**
     * Return query array formatted for using in MongoDb queries
     *
     * @return array
     */
    public function getQueryArray();
}