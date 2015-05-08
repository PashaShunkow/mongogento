<?php
/**
 * Smile MongoCatalog 'AND' condition model
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2015 Smile
 */
class Smile_MongoCatalog_Model_Condition_And extends Smile_MongoCatalog_Model_Condition
{
    const CONDITION_TYPE = 'AND';

    protected $_attributeData = null;

    protected $_conditionData = null;

    /**
     * Init an instance of the model
     *
     * @param $attributeData
     * @param $conditionData
     *
     * @return Smile_MongoCatalog_Model_Condition_And
     */
    public function initCondition($attributeData, $conditionData){
        $this->_attributeData = $attributeData;
        $this->_conditionData = $conditionData;
        return $this;
    }

    /**
     * Return query array formatted for using in MongoDb queries
     *
     * @return array
     */
    public function getQueryArray(){
        $conditions           = array();
        $dataType             = null;
        $this->_attributeCode = $this->_attributeData;
        $scopedAttributeName  = $this->_getDataHelper()->getMongoAttributeName($this->_attributeData, true);
        $globalAttributeName  = $this->_getDataHelper()->getMongoAttributeName($this->_attributeData, false);

        foreach($this->_conditionData as $type => $value){
            if ($this->_isTypeFlag($type)) {
                $dataType = $type;
                unset($this->_conditionData[$type]);
                break;
            }
        }

        foreach ($this->_conditionData as $type => $value) {
            $conditions[] = $this->_buildSubCondition($type, $value, $dataType);
        }

        $conditions = $this->_prepareConditions($conditions);
        $result     = $this->_getQueryArrayTemplate($globalAttributeName, $scopedAttributeName);
        $result[static::MONGO_OPERATOR_OR][0]['$and'][] = array($scopedAttributeName => $conditions);
        $result[static::MONGO_OPERATOR_OR][1]['$and'][] = array($globalAttributeName => $conditions);
        return $result;
    }

    /**
     * Prepare condition array for use in Mongo as 'and' condition
     * for one attribute
     *
     * @param array $conditionsArray
     *
     * @return array
     */
    protected function _prepareConditions(array $conditionsArray){
        foreach($conditionsArray as $key => $condition){
            foreach($condition as $conditionKey => $conditionValue){
                $conditionsArray[$conditionKey] = $conditionValue;
            }
            unset($conditionsArray[$key]);
        }
        return $conditionsArray;
    }
}