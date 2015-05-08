<?php
/**
 * Smile MongoCatalog 'DEFAULT' condition model
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2015 Smile
 */
class Smile_MongoCatalog_Model_Condition_Default extends Smile_MongoCatalog_Model_Condition
{
    const CONDITION_TYPE = 'DEFAULT';

    protected $_attributeData = null;

    protected $_conditionData = null;
    /**
     * Init an instance of the model
     *
     * @param $attributeData
     * @param $conditionData
     *
     * @return Smile_MongoCatalog_Model_Condition_Default
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
        $this->_attributeCode = $this->_attributeData;
        $scopedAttributeName  = $this->_getDataHelper()->getMongoAttributeName($this->_attributeCode, true);
        $globalAttributeName  = $this->_getDataHelper()->getMongoAttributeName($this->_attributeCode, false);

        if (!is_array($this->_conditionData) || !$this->isAssocArray($this->_conditionData)) {
            $this->_conditionData = array('eq' => $this->_conditionData);
        }
        list($type)   = array_keys($this->_conditionData);
        $value        = $this->_conditionData[$type];
        $conditions   = $this->_buildSubCondition($type, $value);
        $result       = $this->_getQueryArrayTemplate($globalAttributeName, $scopedAttributeName);
        $result[static::MONGO_OPERATOR_OR][0][static::MONGO_OPERATOR_AND][] = array($scopedAttributeName => $conditions);
        $result[static::MONGO_OPERATOR_OR][1][static::MONGO_OPERATOR_AND][] = array($globalAttributeName => $conditions);
        return $result;
    }
}