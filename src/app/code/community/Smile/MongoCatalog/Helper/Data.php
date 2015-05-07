<?php
/**
 * Smile Mongo catalog data helper
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Paul Shunkow <pashu@smile.fr>
 * @copyright 2015 Smile
 */
class Smile_MongoCatalog_Helper_Data extends Mage_Core_Helper_Abstract
{
    /**
     * Return Mongo attribute name
     *
     * @param string $attributeName Magento attribute code
     * @param bool   $scoped        Set 0 if you need global attribute name or set 1 if you need attribute name for current store
     *
     * @return null|string
     */
    public function getMongoAttributeName($attributeName, $scoped = false)
    {
        $result = null;
        $template = 'attr_%s.%s';
        $scopedAttributeName = sprintf($template, Mage::app()->getStore()->getStoreId(), $attributeName);
        $globalAttributeName = sprintf($template, Mage_Core_Model_App::ADMIN_STORE_ID, $attributeName);
        if ($scoped) {
            $result = $globalAttributeName;
        } else {
            $result = $scopedAttributeName;
        }
        return $result;
    }
}