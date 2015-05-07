<?php
/**
 * MongoGento
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the Academic Free License (AFL 3.0)
 * that is bundled with this package in the file LICENSE_AFL.txt.
 * It is also available through the world-wide-web at this URL:
 * http://opensource.org/licenses/afl-3.0.php
 *
 * DISCLAIMER
 *
 * Do not edit or add to this file if you wish to upgrade MongoGento to newer
 * versions in the future.
 */

/**
 * Product collection handling with data loading from MongoDB
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>, Paul Shunkow <pashu@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Resource_Override_Catalog_Product_Collection extends Mage_Catalog_Model_Resource_Product_Collection
{
    /**
     * Mongo logic operators
     */
    const MONGO_OPERATOR_OR  = '$or';
    const MONGO_OPERATOR_AND = '$and';

    /**
     * Types of filter conditions
     */
    const CONDITION_TYPE_DEFAULT = 'DEFAULT';
    const CONDITION_TYPE_OR      = 'OR';
    const CONDITION_TYPE_AND     = 'AND';
    /**
     * This collection is used to access to the product collection into MongoDB
     *
     * @var MongoCollection
     */
    protected $_docCollection;


    /**
     * This field is intended to store all attribute filtered that are not MySQL attributes
     *
     * @var array
     */
    protected $_documentFilters = array();


    /**
     * This field indicates if some SQL filters (attributes) have been applied to the collection
     *
     * @var bool
     */
    protected $_hasSqlFilter = false;

    /**
     * When loading collection, all docs loaded from Mongo are kept into this field
     *
     * @var null|array
     */
    protected $_loadedDocuments = null;

    /**
     * Flag to replace category ids with their instance
     * Default not loaded, the id remain interger
     */
    protected $_addMainCategories = false;

    /**
     * Result array with filter conditions
     *
     * @var array
     */
    protected $_result = array();

    /**
     * Mongo equivalents of Magento Sql operators
     *
     * @var array
     */
    protected $_operatorsMap = array(
        'gteq'    => 'gte',
        'moreq'   => 'gte',
        'from'    => 'gte',
        'lteq'    => 'lte',
        'neq'     => 'ne',
        'notnull' => 'ne',
        'to'      => 'lt',
        'like'    => 'regexp',
        'finset'  => 'in'
    );

     /**
     * Set flag to load main category
     *
     * @param bool $add flag to set
     *
     * @return void
     */
    public function addMainCategory($add = true)
    {
        $this->_addMainCategories = $add;
    }

    /**
     * Set flag to replace category ids with their instance
     *
     * @return bool $replace flag
     */
    public function mustLoadMainCategory()
    {
        return $this->_addMainCategories;
    }


    /**
     * Processing collection items after loading
     * Raise additional event
     *
     * @return Mage_Catalog_Model_Resource_Product_Collection
     */
    protected function _afterLoad()
    {
        parent::_afterLoad();
        if (count($this) > 0) {
            Mage::dispatchEvent('mongo_catalog_product_collection_load_after', array('collection' => $this));
        }
        return $this;
    }

    /**
     * Use the mongo adapter to get access to a collection used as storage for products.
     * The collection used has the same name as main entity table (catalog_product_entity).
     *
     * @return MongoCollection The Mongo document collection
     */
    protected function _getDocumentCollection()
    {
        if (is_null($this->_docCollection)) {
            $adapter = Mage::getSingleton('mongocore/resource_connection_adapter');
            $collectionName = $this->getResource()->getEntityTable();
            $this->_docCollection = $adapter->getCollection($collectionName);
        }

        return $this->_docCollection;
    }

    /**
     * Load attributes from MongoDB after main data are loaded from MySQL
     *
     * @param bool $printQuery If the SQL query should be printed or not
     * @param bool $logQuery   If the SQL query should be logegd or not
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product_Collection Self reference
     */
    public function _loadAttributes($printQuery = false, $logQuery = false)
    {
        parent::_loadAttributes($printQuery, $logQuery);

        if (!empty($this->_itemsById)) {

            $storeFilter = array_unique(array('attr_' . $this->getDefaultStoreId(), 'attr_' . $this->getStoreId()));

            if (is_null($this->_loadedDocuments)) {

                $documentIds = $this->getLoadedIds();

                foreach ($documentIds as $key => $value) {
                    $documentIds[$key] = new MongoInt32($value);
                }

                $idFilter = array('_id' => array('$in' => $documentIds));

                $cursor = $this->_getDocumentCollection()
                    ->find($idFilter, $storeFilter);

                $this->_loadedDocuments = array();

                while ($cursor->hasNext()) {
                    $document = $cursor->getNext();
                    $this->_loadedDocuments[] = $document;
                }
            }

            foreach ($this->_loadedDocuments as $document) {
                $loadedData = array();
                //$document = $cursor->getNext();

                foreach ($storeFilter as $storeId) {
                    if (isset($document[$storeId])) {
                        if (!is_array($document[$storeId])) {
                            $document[$storeId] = array($storeId=>$document[$storeId]);
                        }
                        foreach ($document[$storeId] as $attributeCode => $attributeValue) {
                            $loadedData[$attributeCode] = $attributeValue;
                        }
                    }
                }

                $this->_items[$document['_id']]->addData($loadedData);
            }

        }

        return $this;
    }


    /**
     * Add attribute filter to collection
     *
     * If $attribute is an array will add OR condition with following format:
     * array(
     *     array('attribute'=>'firstname', 'like'=>'test%'),
     *     array('attribute'=>'lastname', 'like'=>'test%'),
     * )
     *
     * @param Mage_Eav_Model_Entity_Attribute_Interface|integer|string|array $attribute The attribute to be filtered
     * @param null|string|array                                              $condition Filter condition array or value
     * @param string                                                         $joinType  Indicate if we deal with inner or left join
     *
     * @return Mage_Eav_Model_Entity_Collection_Abstract Self reference
     */
    public function addAttributeToFilter($attribute, $condition = null, $joinType = 'inner')
    {
        if ($attribute === null) {
            $this->getSelect();
            return $this;
        }

        if (is_numeric($attribute)) {
            $attribute = $this->getEntity()->getAttribute($attribute)->getAttributeCode();
        } else if ($attribute instanceof Mage_Eav_Model_Entity_Attribute_Interface) {
            $attribute = $attribute->getAttributeCode();
        }

        $sqlAttributes = $this->getResource()->getSqlAttributesCodes();

        if (is_array($attribute)) {
            $sqlArr = array();
            foreach ($attribute as $condition) {
                if ($this->getAttribute($condition['attribute']) === false || in_array($condition['attribute'], $sqlAttributes)) {
                    $sqlArr[] = $this->_getAttributeConditionSql($condition['attribute'], $condition, $joinType);
                }
            }
            if(empty($sqlArr)){
                $this->_addDocumentFilter($attribute, $condition, $this->_getConditionType($attribute));
            }else{
                $conditionSql = '('.implode(') OR (', $sqlArr).')';
            }
        } else if (is_string($attribute)) {
            if ($condition === null) {
                $condition = '';
            }

            if ($this->getAttribute($attribute) === false || in_array($attribute, $sqlAttributes)) {
                $conditionSql = $this->_getAttributeConditionSql($attribute, $condition, $joinType);
            } else {
                $this->_addDocumentFilter($attribute, $condition, $this->_getConditionType($attribute, $condition));
            }
        }

        if (!empty($conditionSql)) {
            $this->_hasSqlFilter = true;
            $this->getSelect()->where($conditionSql, null, Varien_Db_Select::TYPE_CONDITION);
        }

        return $this;
    }

    /**
     * Return condition type based on condition structure
     *
     * @param mixed      $attribute
     * @param null|array $condition
     *
     * @return string
     */
    protected function _getConditionType($attribute, $condition = null){
        $conditionType = static::CONDITION_TYPE_DEFAULT;
        if(is_array($attribute)){
            $conditionType = static::CONDITION_TYPE_OR;
        }
        if(is_string($attribute) && is_array($condition) && $this->isAssocArray($condition) && count($condition) > 1){
            $conditionType = static::CONDITION_TYPE_AND;
        }
        return $conditionType;
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
     * Append a filter to be applied on DOCUMENTS (MongoDB) when loading the collection
     *
     * @param Mage_Eav_Model_Entity_Attribute_Interface|integer|string|array $attribute      The attribute to be filtered
     * @param null|string|array                                              $condition      Filter condition array or value
     * @param string                                                         $conditionType  Show conditional type AND | OR
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product_Collection Self reference
     */
    protected function _addDocumentFilter($attribute, $condition, $conditionType)
    {
        $this->_documentFilters[][$conditionType] = array('attribute' => $attribute, 'condition' => $condition);
        return $this;
    }

    /**
     * Apply MongoDB filtering before loading the collection
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product_Collection Self reference
     */
    protected function _beforeLoad()
    {
        parent::_beforeLoad();

        $documentFilter = array();

        foreach ($this->_documentFilters as $attribute ) {
            $filter = $this->_buildDocumentFilter($attribute);

            if (!is_null($filter)) {
                $documentFilter[] = $filter;
            }
        }

        if (!empty($documentFilter)) {

            $productIds = null;

            if ($this->_hasSqlFilter !== false) {
                $productIds = $this->getAllIds();
            }

            $documentIds = array();

            if (!is_null($productIds) && !empty($productIds)) {
                $documentFilter = array('$and' => array(
                    $this->getResource()->getIdsFilter($productIds),
                    array('$and' => $documentFilter)
                ));
            } else {
                $documentFilter = array('$and' => array_values($documentFilter));
            }

            $storeFilter = array_unique(array('attr_' . $this->getDefaultStoreId(), 'attr_' . $this->getStoreId()));

            $cursor = $this->_getDocumentCollection()
                ->find($documentFilter, $storeFilter)
                ->limit($this->getPageSize());

            while ($cursor->hasNext()) {
                $document = $cursor->getNext();
                $documentIds[] = $document['_id'];
            }

            $this->getSelect()->where('e.entity_id IN(?)', $documentIds);
        }
    }

    /**
     * Build Mongo filter for a an attribute. Following Magento filters are supported :
     *
     * - array("from" => $fromValue, "to" => $toValue)    [OK]
     * - array("eq" => $equalValue)                       [OK]
     * - array("neq" => $notEqualValue)                   [OK]
     * - array("like" => $likeValue)                      [OK]
     * - array("in" => array($inValues))                  [OK]
     * - array("nin" => array($notInValues))              [OK]
     * - array("notnull" => $valueIsNotNull)              [OK]
     * - array("null" => $valueIsNull)                    [OK]
     * - array("moreq" => $moreOrEqualValue)              [OK]
     * - array("gt" => $greaterValue)                     [OK]
     * - array("lt" => $lessValue)                        [OK]
     * - array("gteq" => $greaterOrEqualValue)            [OK]
     * - array("lteq" => $lessOrEqualValue)               [OK]
     * - array("finset" => $valueInSet)                   [OK]
     * - array("regexp" => $regularExpression)            [OK]
     * - array("seq" => $stringValue)                     [OK]
     * - array("sneq" => $stringValue)                    [OK]
     *
     * @param Mage_Eav_Model_Entity_Attribute_Interface|integer|string|array $attribute     The attribute to be filtered, conditions info
     *
     * @return array The Filter to be applied
     */
    protected function _buildDocumentFilter($attribute)
    {
        list($conditionType, $condition, $attributeName) = $this->_getAttributeFilterInfo($attribute);

        $scopedAttributeName = $this->_getDataHelper()->getMongoAttributeName($attributeName, true);
        $globalAttributeName = $this->_getDataHelper()->getMongoAttributeName($attributeName, false);

        $result = $this->_getQueryArray($conditionType, $globalAttributeName, $scopedAttributeName);

        $conditions = array();

        if ($conditionType == static::CONDITION_TYPE_AND) {
            /**
             * Here will be processes code if used 'AND' condition in magento collection
             * example:
             *  addAttributeToFilter('my_new_date',
             *    array('from' => '2014-05-01', 'to' => '2018-04-12')
             *  );
             */
            foreach($condition as $type => $value){
                $conditions[] = $this->_buildCondition($type, $value);
            }
            $conditions = $this->_prepareAndCondition($conditions);
            $result[static::MONGO_OPERATOR_OR][0]['$and'][] = array($scopedAttributeName => $conditions);
            $result[static::MONGO_OPERATOR_OR][1]['$and'][] = array($globalAttributeName => $conditions);
        } else if ($conditionType == static::CONDITION_TYPE_OR) {
            /**
             * Here will be processes code if used 'OR' condition in magento collection
             * example:
             * addAttributeToFilter(
             *  array(
             *   array('attribute'=> 'color','in' => array(23,22)),
             *   array('attribute'=> 'my_new_date','from' => '2014-05-01')
             *  )
             * );
             */
            foreach($condition as $item){
                $attributeName = $item['attribute'];
                unset($item['attribute']);
                list($type) = array_keys($item);
                $value      = $item[$type];
                $conditions = $this->_buildCondition($type, $value);
                $scopedAttributeName = $this->_getDataHelper()->getMongoAttributeName($attributeName, true);
                $globalAttributeName = $this->_getDataHelper()->getMongoAttributeName($attributeName, false);

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
        } else {
            /**
             * Here will be processes code if used 'DEFAULT' condition in magento collection
             * example:
             * addAttributeToFilter('attribute', 'value'));
             * or
             * addAttributeToFilter('attribute', array('value','value2'));
             */
            if (!is_array($condition) || !$this->isAssocArray($condition)) {
                $condition = array('eq' => $condition);
            }
            list($type)   = array_keys($condition);
            $value        = $condition[$type];
            $conditions   = $this->_buildCondition($type, $value);
            $result[static::MONGO_OPERATOR_OR][0][static::MONGO_OPERATOR_AND][] = array($scopedAttributeName => $conditions);
            $result[static::MONGO_OPERATOR_OR][1][static::MONGO_OPERATOR_AND][] = array($globalAttributeName => $conditions);
        }

        return $result;
    }

    protected function _getQueryArray($conditionType, $globalAttributeName, $scopedAttributeName){
        if ($conditionType == static::CONDITION_TYPE_OR) {
            $result = array();
        } else {
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
        }
        return $result;
    }

    /**
     * Convert attribute data into attribute info array
     *
     * @param array $attribute Attribute data array
     *
     * @return array
     */
    protected function _getAttributeFilterInfo(array $attribute){
        $info = array();
        if(isset($attribute[static::CONDITION_TYPE_OR])){
            $info = array('OR', $attribute[static::CONDITION_TYPE_OR]['attribute'], false);
        }
        if(isset($attribute[static::CONDITION_TYPE_AND])){
            $info = array(static::CONDITION_TYPE_AND, $attribute[static::CONDITION_TYPE_AND]['condition'], $attribute[static::CONDITION_TYPE_AND]['attribute']);
        }
        if(isset($attribute[static::CONDITION_TYPE_DEFAULT])){
            $info = array(static::CONDITION_TYPE_DEFAULT, $attribute[static::CONDITION_TYPE_DEFAULT]['condition'], $attribute[static::CONDITION_TYPE_DEFAULT]['attribute']);
        }
        return $info;
    }

    /**
     * Build condition for using in Mongo collection
     *
     * @param string $type  Condition type
     * @param mixed  $value Condition value
     *
     * @return array|null
     */
    protected function _buildCondition($type, $value){
        $condition = array();
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
                /** @var $dateHelper Smile_MongoCatalog_Helper_Date */
                $dateHelper = Mage::helper('smile_mongocatalog/date');
                $condition  = array($this->_convertOperator($type) => $dateHelper->getMongoDateFormat((string) $value));
                break;
            case 'to':
                /** @var $dateHelper Smile_MongoCatalog_Helper_Date */
                $dateHelper = Mage::helper('smile_mongocatalog/date');
                $condition  = array($this->_convertOperator($type) => $dateHelper->getMongoDateFormat((string) $value));
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
     * Prepare condition array for use in Mongo as 'and' condition
     * for one attribute
     *
     * @param array $conditionsArray
     *
     * @return array
     */
    protected function _prepareAndCondition(array $conditionsArray){
        foreach($conditionsArray as $key => $condition){
            foreach($condition as $conditionKey => $conditionValue){
                $conditionsArray[$conditionKey] = $conditionValue;
            }
            unset($conditionsArray[$key]);
        }
        return $conditionsArray;
    }

    /**
     * Return MongoCatalog data helper
     *
     * @return Smile_MongoCatalog_Helper_Data
     */
    protected function _getDataHelper(){
        return Mage::helper('smile_mongocatalog');
    }

    /**
     * Since we use MongoDb this method is useless and we always use *
     *
     * @param array|string|integer|Mage_Core_Model_Config_Element $attribute attribute
     * @param false|string                                        $joinType  flag for joining attribute
     *
     * @return  Mage_Eav_Model_Entity_Collection_Abstract
     */
    public function addAttributeToSelect($attribute, $joinType = false)
    {
        return $this;
    }

}
