<?php
/**
 * Smile MongoCatalog common condition model
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2014 Smile
 */
class Smile_MongoCatalog_Model_Condition extends Varien_Object implements Smile_MongoCatalog_Model_Condition_Interface
{
    const CONDITION_TYPE = '';

    protected $_attributeCode = null;

    /**
     * Data types witch needs to be preprocessed
     *
     * @var array
     */
    protected $_dataTypes = array('date');

    /**
     * Types of filter conditions
     */
    const CONDITION_TYPE_DEFAULT = 'DEFAULT';
    const CONDITION_TYPE_OR      = 'OR';
    const CONDITION_TYPE_AND     = 'AND';

    /**
     * Mongo logic operators
     */
    const MONGO_OPERATOR_OR  = '$or';
    const MONGO_OPERATOR_AND = '$and';

    /**
     * Mongo equivalents of Magento Sql operators
     *
     * @var array
     */
    protected $_operatorsMap = array(
        'gteq'     => 'gte',
        'moreq'    => 'gte',
        'from'     => 'gte',
        'lteq'     => 'lte',
        'neq'      => 'ne',
        'notnull'  => 'ne',
        'not null' => 'ne',
        'to'       => 'lt',
        'like'     => 'regex',
        'regexp'   => 'regex',
        'finset'   => 'in'
    );

    /**
     * Find particular condition model, create and init an instance of the model
     *
     * @param $attributeData
     * @param $conditionData
     *
     * @return false|Smile_MongoCatalog_Model_Condition_And|Smile_MongoCatalog_Model_Condition_Or
     */
    public function initCondition($attributeData, $conditionData){
        $conditionType  = $this->_getConditionType($attributeData, $conditionData);
        /** @var $conditionModel Smile_MongoCatalog_Model_Condition_And|Smile_MongoCatalog_Model_Condition_Or */
        $conditionModel = Mage::getModel('mongocatalog/condition_' . strtolower($conditionType));
        if(!$conditionModel){
            Mage::throwException("Cant load model for '{$conditionType}' condition");
        }
        $conditionModel->initCondition($attributeData, $conditionData);
        return $conditionModel;
    }

    /**
     * Return condition type
     *
     * @return string
     */
    public function getConditionType(){
        return static::CONDITION_TYPE;
    }

    /**
     * Return query array formatted for using in MongoDb queries
     *
     * @return array
     */
    public function getQueryArray(){
        return array();
    }

    /**
     * Return condition type based on condition structure
     *
     * @param mixed      $attributeData
     * @param null|array $conditionData
     *
     * @return string
     */
    protected function _getConditionType($attributeData, $conditionData = null){
        $conditionType = static::CONDITION_TYPE_DEFAULT;
        if(is_array($attributeData) || isset($conditionData['or'])){
            $conditionType = static::CONDITION_TYPE_OR;
        }
        if(is_string($attributeData) && is_array($conditionData) && $this->isAssocArray($conditionData) && count($conditionData) > 1){
            $conditionType = static::CONDITION_TYPE_AND;
        }
        return $conditionType;
    }

    /**
     * Return common part of condition array
     *
     * @param string $globalAttributeName Global attribute name
     * @param string $scopedAttributeName Scoped attribute name
     *
     * @return array
     */
    protected function _getQueryArrayTemplate($globalAttributeName, $scopedAttributeName){
        $result = array(
            static::MONGO_OPERATOR_OR => array(
                array(static::MONGO_OPERATOR_AND => array(
                    array($scopedAttributeName => array('$exists' => 1)),

                )),
                array(static::MONGO_OPERATOR_AND => array(
                    array($scopedAttributeName => array('$exists' => 0)),
                    array($globalAttributeName => array('$exists' => 1)),
                )),
            )
        );
        return $result;
    }

    /**
     * Build condition for using in Mongo collection
     *
     * @param string      $type     Condition type
     * @param mixed       $value    Condition value
     * @param string|null $dataType Data type
     *
     * @return array|null
     */
    protected function _buildSubCondition($type, $value, $dataType = null){
        $condition = array();
        $value     = $this->_processValueByDataType($value, $dataType);

        switch ($type) {
            case 'like':
                $regexp    = new MongoRegex('/' . str_replace(array('\'%','%\''), '.*', $value) . '/i');
                $condition = array($this->_convertOperator($type) => $regexp);
                break;
            case 'eq':
                if(is_array($value)){
                    $condition = array($this->_convertOperator('in') => $value);
                }else{
                    $condition = $value;
                }
                break;
            case 'gt':
            case 'gteq':
            case 'lt':
            case 'lteq':
            case 'moreq':
            case 'neq':
            case 'in':
            case 'nin':
            case 'finset':
                $condition = array($this->_convertOperator($type) => $value);
                break;
            case 'notnull':
            case 'not null':
                $condition = array($this->_convertOperator($type) => null);
                break;
            case 'null':
                $condition = null;
                break;
            case 'regexp':
                $regexp    = new MongoRegex('/' . $condition[$type] . '/i');
                $condition = array($this->_convertOperator($type) => $regexp);
                break;
            case 'seq':
                if ($value == '') {
                    $condition = null;
                } else {
                    $condition = $value;
                }
                break;
            case 'sneq':
                if ($condition[$type] == '') {
                    $type      = '$ne';
                    $condition = array($type => null);
                } else {
                    $condition = array($this->_convertOperator($type) => (string) $value);
                }
                break;
            case 'from':
                $condition  = array($this->_convertOperator($type) => $value);
                break;
            case 'to':
                $condition  = array($this->_convertOperator($type) => $value);
                break;
            default:
                // @FIX
                $file = __FILE__;
                Mage::throwException("{$file} {$type} : unsuported MongoDB attribute filter");
                break;
        }

        return $condition;
    }

    /**
     * Convert sql magento operators to mongo operators
     *
     * @param string $operator SqlMagento Operator
     *
     * @return string
     */
    protected function _convertOperator($operator)
    {
        if (isset($this->_operatorsMap[$operator])) {
            $operator = $this->_operatorsMap[$operator];
        }
        return sprintf('$%s', $operator);
    }

    /**
     * Process value for specific data types
     *
     * @param      $value
     * @param null $dataType
     *
     * @return string
     */
    protected function _processValueByDataType($value, $dataType = null){
        if($dataType == null){
            $attributeModel = Mage::getModel('eav/entity_attribute')->loadByCode('catalog_product', $this->_attributeCode);
            $dataType       = $attributeModel->getBackendType();
        }
        switch ($dataType) {
            case 'date':
            case 'datetime':
                /** @var $dateHelper Smile_MongoCatalog_Helper_Date */
                $dateHelper = Mage::helper('smile_mongocatalog/date');
                if (is_array($value)) {
                    foreach ($value as $key => $val) {
                        $value[$key] = $dateHelper->getMongoDateFormat($val, $dataType);
                    }
                } else {
                    $value = $dateHelper->getMongoDateFormat($value);
                }
                break;
            default:
                break;
        }
        return $value;
    }

    /**
     * Check if string is attribute data type
     *
     * @param $value
     *
     * @return bool
     */
    protected function _isTypeFlag($value){
        $result = false;
        if(in_array($value, $this->_dataTypes)){
            $result = true;
        }
        return $result;
    }

    /**
     * Check if array is assoc
     *
     * @param array $array Input array
     *
     * @return bool
     */
    protected function isAssocArray($array)
    {
        $keys = array_keys($array);
        return array_keys($keys) !== $keys;
    }

    /**
     * Return MongoCatalog data helper
     *
     * @return Smile_MongoCatalog_Helper_Data
     */
    protected function _getDataHelper(){
        return Mage::helper('smile_mongocatalog');
    }
}