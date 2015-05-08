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
                $this->_addDocumentFilter($this->_buildFilter($attribute, $condition));
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
                $this->_addDocumentFilter($this->_buildFilter($attribute, $condition));
            }
        }

        if (!empty($conditionSql)) {
            $this->_hasSqlFilter = true;
            $this->getSelect()->where($conditionSql, null, Varien_Db_Select::TYPE_CONDITION);
        }

        return $this;
    }

    /**
     * Return filter object for condition
     *
     * @param mixed $attribute Attribute data
     * @param mixed $condition Condition data
     *
     * @return Smile_MongoCatalog_Model_Condition
     */
    protected function _buildFilter($attribute, $condition){
        return Mage::getModel('mongocatalog/condition')->initCondition($attribute, $condition);
    }

    /**
     * Append a filter to be applied on DOCUMENTS (MongoDB) when loading the collection
     *
     * @param Smile_MongoCatalog_Model_Condition $filter Filter object
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product_Collection Self reference
     */
    protected function _addDocumentFilter(Smile_MongoCatalog_Model_Condition $filter)
    {
        $this->_documentFilters[] = $filter;
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

        foreach ($this->_documentFilters as $filterData ) {
            $filter = $this->_buildDocumentFilter($filterData);

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
     * @param Smile_MongoCatalog_Model_Condition $filterDataObject The attribute to be filtered, conditions info
     *
     * @return array The Filter to be applied
     */
    protected function _buildDocumentFilter(Smile_MongoCatalog_Model_Condition $filterDataObject)
    {
        return $filterDataObject->getQueryArray();
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
