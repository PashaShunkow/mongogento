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
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Resource_Override_Catalog_Product_Collection extends Mage_Catalog_Model_Resource_Product_Collection
{
    const MONGO_OPERATOR_OR  = '$or';

    const MONGO_OPERATOR_AND = '$and';
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
            'gteq'  => 'gte',
            'moreq' => 'gte',
            'from'  => 'gte',
            'lteq'  => 'lte',
            'neq'   => 'ne',
            'to'    => 'lt'
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

        unset($sqlAttributes[18]);

        if (is_array($attribute)) {
            $sqlArr = array();
            foreach ($attribute as $condition) {
                if ($this->getAttribute($condition['attribute']) === false || in_array($condition['attribute'], $sqlAttributes)) {
                    $sqlArr[] = $this->_getAttributeConditionSql($condition['attribute'], $condition, $joinType);
                } else {
                    $this->_addDocumentFilter($condition['attribute'], $condition, $joinType);
                }
            }
            $conditionSql = '('.implode(') OR (', $sqlArr).')';
        } else if (is_string($attribute)) {
            if ($condition === null) {
                $condition = '';
            }

            if ($this->getAttribute($attribute) === false || in_array($attribute, $sqlAttributes)) {
                $conditionSql = $this->_getAttributeConditionSql($attribute, $condition, $joinType);
            } else {
                $this->_addDocumentFilter($attribute, $condition, $joinType);
            }
        }

        if (!empty($conditionSql)) {
            $this->_hasSqlFilter = true;
            $this->getSelect()->where($conditionSql, null, Varien_Db_Select::TYPE_CONDITION);
        }

        return $this;
    }

    /**
     * Append a filter to be applied on DOCUMENTS (MongoDB) when loading the collection
     *
     * @param Mage_Eav_Model_Entity_Attribute_Interface|integer|string|array $attribute The attribute to be filtered
     * @param null|string|array                                              $condition Filter condition array or value
     * @param string                                                         $joinType  Indicate if we deal with inner or left join
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product_Collection Self reference
     */
    protected function _addDocumentFilter($attribute, $condition, $joinType)
    {
        $this->_documentFilters[$attribute] = array('condition' => $condition, 'joinType' => $joinType);
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

        foreach ($this->_documentFilters as $attribute => $filter) {
            $filter = $this->_buildDocumentFilter($attribute, $filter);

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
                var_dump($document);
            }
            //$log = $cursor->explain();
            //var_dump(json_encode($log['queryPlanner']['parsedQuery']));
            die('1');

            $this->getSelect()->where('e.entity_id IN(?)', $documentIds);
        }
    }

    /**
     * Build Mongo filter for a an attribute. Following Magento filters are supported :
     *
     * - array("from" => $fromValue, "to" => $toValue)    [NOT IMPLEMENTED]
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
     * - array("finset" => $valueInSet)                   [NOT IMPLEMENTED]
     * - array("regexp" => $regularExpression)            [OK]
     * - array("seq" => $stringValue)                     [OK]
     * - array("sneq" => $stringValue)                    [OK]
     *
     * @param Mage_Eav_Model_Entity_Attribute_Interface|integer|string|array $attribute     The attribute to be filtered
     * @param null|string|array                                              $filter        Filter condition array or value
     * @param string                                                         $logicOperator Could be specified as $or/$and
     *
     * @return array The Filter to be applied
     */
    protected function _buildDocumentFilter($attribute, $filter, $logicOperator = '$or')
    {
        $condition = $filter['condition'];

        if (!is_array($condition)) {
            $condition = array('eq' => $condition);
        }

        if (count($condition) > 1 && !(isset($condition['from']) && isset($condition['to']))) {
            $this->_setResultArray(array(static::MONGO_OPERATOR_OR => array()));
            foreach ($condition as $currentCondition) {
                $this->_result[static::MONGO_OPERATOR_OR][] = $this->_buildDocumentFilter($attribute, $currentCondition);
            }
        } else {
            if(isset($condition['from']) && isset($condition['to'])){
                $condition = array(
                    'from-to' => $condition['from'] . ';' . $condition['to']
                );
            }
            list($type) = array_keys($condition);

            $scopedAttributeName = 'attr_' . $this->getStoreId() . '.' . $attribute;
            $globalAttributeName = 'attr_' . Mage_Core_Model_App::ADMIN_STORE_ID . '.' . $attribute;
            $resultCascade = array(
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

            $this->_setResultArray($resultCascade);

            switch ($type) {
                case 'or':
                case 'and':
                    $this->_setResultArray(array("${$type}" => array()));
                    foreach ($condition as $currentCondition) {
                        $this->_result['${$type}'][] = $this->_buildDocumentFilter($attribute, $currentCondition);
                    }
                    break;
                case 'like':
                    $regexp = new MongoRegex('/' . str_replace(array('\'%','%\''), '.*', $condition[$type]) . '/i');
                    $this->_addToResult(array($scopedAttributeName => array('$regex' => $regexp)), 0);
                    $this->_addToResult(array($globalAttributeName => array('$regex' => $regexp)), 1);
                    break;
                case 'eq':
                    $this->_addToResult(array($scopedAttributeName => $condition[$type]), 0);
                    $this->_addToResult(array($globalAttributeName => $condition[$type]), 1);
                    break;
                case 'gt':
                case 'gteq':
                case 'lt':
                case 'lteq':
                case 'moreq':
                case 'neq':
                    $filterValue = (string)$condition[$type];
                    $type = $this->_convertOperator($type);
                    $this->_addToResult(array($scopedAttributeName => array('$' . $type => $filterValue)), 0);
                    $this->_addToResult(array($globalAttributeName => array('$' . $type => $filterValue)), 1);
                    break;
                case 'in':
                    $this->_addToResult(array($scopedAttributeName => array('$' . $type => $condition[$type])), 0);
                    $this->_addToResult(array($globalAttributeName => array('$' . $type => $condition[$type])), 1);
                    break;
                case 'nin':
                    $this->_addToResult(array($scopedAttributeName => array('$' . $type => $condition[$type])), 0);
                    $this->_addToResult(array($globalAttributeName => array('$' . $type => $condition[$type])), 1);
                    break;
                case 'notnull':
                    $type   = 'ne';
                    $this->_addToResult(array($scopedAttributeName => array('$' . $type => null)), 0);
                    $this->_addToResult(array($globalAttributeName => array('$' . $type => null)), 1);
                    break;
                case 'null':
                    $this->_addToResult(array($scopedAttributeName => null), 0);
                    $this->_addToResult(array($globalAttributeName => null), 1);
                    break;
                case 'regexp':
                    $regexp = new MongoRegex('/' . $condition[$type] . '/i');
                    $this->_addToResult(array($scopedAttributeName => array('$regex' => $regexp)), 0);
                    $this->_addToResult(array($globalAttributeName => array('$regex' => $regexp)), 1);
                    break;
                case 'seq':
                    if ($condition[$type] == '') {
                        $this->_addToResult(array($scopedAttributeName => null), 0);
                        $this->_addToResult(array($globalAttributeName => null), 1);
                    } else {
                        $this->_addToResult(array($scopedAttributeName => $condition[$type]), 0);
                        $this->_addToResult(array($globalAttributeName => $condition[$type]), 1);
                    }
                    break;
                case 'sneq':
                    if ($condition[$type] == '') {
                        $type   = 'ne';
                        $this->_addToResult(array($scopedAttributeName => array('$' . $type => null)), 0);
                        $this->_addToResult(array($globalAttributeName => array('$' . $type => null)), 1);
                    } else {
                        $filterValue = (string)$condition[$type];
                        $type = $this->_convertOperator($type);
                        $this->_addToResult(array($scopedAttributeName => array('$' . $type => $filterValue)), 0);
                        $this->_addToResult(array($globalAttributeName => array('$' . $type => $filterValue)), 1);
                    }
                    break;
                case 'from':
                    /** @var $dateHelper Smile_MongoCatalog_Helper_Date */
                    $dateHelper  = Mage::helper('smile_mongocatalog/date');
                    $filterValue = (string)$condition[$type];
                    $type = $this->_convertOperator($type);
                    $this->_addToResult(array($scopedAttributeName => array('$' . $type => $dateHelper->getMongoDateFormat($filterValue))), 0);
                    $this->_addToResult(array($globalAttributeName => array('$' . $type => $dateHelper->getMongoDateFormat($filterValue))), 1);
                    break;
                case 'to':
                    /** @var $dateHelper Smile_MongoCatalog_Helper_Date */
                    $dateHelper  = Mage::helper('smile_mongocatalog/date');
                    $filterValue = (string)$condition[$type];
                    $type = $this->_convertOperator($type);
                    $this->_addToResult(array($scopedAttributeName => array('$' . $type => $dateHelper->getMongoDateFormat($filterValue))), 0);
                    $this->_addToResult(array($globalAttributeName => array('$' . $type => $dateHelper->getMongoDateFormat($filterValue))), 1);
                    break;
                case 'from-to':
                    /** @var $dateHelper Smile_MongoCatalog_Helper_Date */
                    $dateHelper  = Mage::helper('smile_mongocatalog/date');
                    list($from, $to) = explode(';', (string)$condition[$type]);
                    $this->_addToResult(
                        array(
                            $scopedAttributeName => array(
                                '$' . $this->_convertOperator('from') => $dateHelper->getMongoDateFormat($from),
                                '$' . $this->_convertOperator('to')   => $dateHelper->getMongoDateFormat($to)
                            )
                        ),
                        0
                    );
                    $this->_addToResult(
                        array(
                            $globalAttributeName => array(
                                '$' . $this->_convertOperator('from') => $dateHelper->getMongoDateFormat($from),
                                '$' . $this->_convertOperator('to')   => $dateHelper->getMongoDateFormat($to)
                            )
                        ),
                        1
                    );
                    break;
                default:
                    // @FIX
                    $file = __FILE__;
                    Mage::throwException("{$file} {$type} : unsuported MongoDB attribute filter");
                    break;
            }
        }

        return $this->_result;
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
        return $operator;
    }

    /**
     * Set result array as into class member
     *
     * @param array $result
     *
     * @return void
     */
    protected function _setResultArray(array $result){
        $this->_result = $result;
    }

    /**
     * Add new conditions to common array
     *
     * @param array $conditions
     * @param int   $scoped
     *
     * @return void
     */
    protected function _addToResult(array $conditions, $scoped = 0)
    {
        $this->_result[static::MONGO_OPERATOR_OR][$scoped]['$and'][] = $conditions;
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
