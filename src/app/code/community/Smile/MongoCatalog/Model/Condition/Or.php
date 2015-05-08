<?php
/**
 * Smile MongoCatalog 'OR' condition model
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2015 Smile
 */
class Smile_MongoCatalog_Model_Condition_Or extends Smile_MongoCatalog_Model_Condition
{
    const CONDITION_TYPE = 'OR';

    protected $_attributeData = null;

    protected $_conditionData = null;
    /**
     * Init an instance of the model
     *
     * @param $attributeData
     * @param $conditionData
     *
     * @return Smile_MongoCatalog_Model_Condition_Or
     */
    public function initCondition($attributeData, $conditionData){
        $this->_attributeData = $attributeData;
        $this->_conditionData = $conditionData;
        $this->_prepareFilterData();
        return $this;
    }

    /**
     * Return query array formatted for using in MongoDb queries
     *
     * @return array
     */
    public function getQueryArray(){
        $result = array();
        foreach($this->_attributeData as $item){
            $this->_attributeCode = $item['attribute'];
            $dataType             = $item['data_type'];
            unset($item['attribute']);
            unset($item['data_type']);

            list($type) = array_keys($item['condition']);
            $value      = $item['condition'][$type];
            $conditions = $this->_buildSubCondition($type, $value, $dataType);

            $scopedAttributeName = $this->_getDataHelper()->getMongoAttributeName($this->_attributeCode, true);
            $globalAttributeName = $this->_getDataHelper()->getMongoAttributeName($this->_attributeCode, false);

            $result[static::MONGO_OPERATOR_OR][][static::MONGO_OPERATOR_AND] = array(
                array($scopedAttributeName => array('$exists' => 1)),
                array($scopedAttributeName => $conditions)
            );
            $result[static::MONGO_OPERATOR_OR][][static::MONGO_OPERATOR_AND] = array(
                array($scopedAttributeName => array('$exists' => 0)),
                array($globalAttributeName => array('$exists' => 1)),
                array($globalAttributeName => $conditions)
            );
        }
        return $result;
    }

    /**
     * Prepare inner keys for use in Mongo as 'and' condition
     * for one attribute
     *
     * @return void
     */
    public function _prepareFilterData(){
            $tmpAttr   = array();
            if (is_string($this->_attributeData) && is_array($this->_conditionData) && isset($this->_conditionData['or'])) {
                $dataArray           = $this->_conditionData['or'];
                $commonAttributeName = $this->_attributeData;
            } else {
                $dataArray           = $this->_attributeData;
                $commonAttributeName = null;
            }
            foreach ($dataArray as $item) {
                if($commonAttributeName == null){
                    $attributeName = $item['attribute'];
                    unset($item['attribute']);
                }else{
                    $attributeName = $commonAttributeName;
                }
                $tmpItem = array(
                    'attribute' => $attributeName,
                    'condition' => array(),
                    'data_type' => null
                );
                foreach ($item as $key => $value) {
                    if($this->_isTypeFlag($key)){
                        $tmpItem['data_type'] = $key;
                    }else{
                        if($key == 'is' && $value instanceof Zend_Db_Expr){
                            $key = $value->__toString();
                            $value = true;
                        }
                        $tmpItem['condition'] = array($key => $value);
                    }
                }
                $tmpAttr[] = $tmpItem;
            }
            $this->_attributeData = $tmpAttr;
            $this->_conditionData = null;
    }
}