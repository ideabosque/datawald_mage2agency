# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from mage2_connector import Mage2Connector


class Mage2Agency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.mage2Connector = Mage2Connector(logger, **setting)
        self.datawald = DatawaldConnector(logger, **setting)
        Agency.__init__(self, logger, datawald=self.datawald)
        if setting.get("tx_type"):
            Agency.tx_type = setting.get("tx_type")

    def tx_asset_tgt(self, asset):
        return asset

    def tx_asset_tgt_ext(self, new_asset, asset):
        pass

    def insert_update_assets(self, assets):
        for asset in assets:
            tx_type = asset.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "product":
                    tgt_id = self.insert_update_product(asset)
                else:
                    raise Exception(f"TX Type ({tx_type}) is not supported!!!")

                asset.update(
                    {
                        "tx_status": "S",
                        "tx_note": f"datawald -> {asset['target']}",
                        "tgt_id": tgt_id,
                    }
                )
            except Exception:
                log = traceback.format_exc()
                asset.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(log)
        return assets

    def insert_update_product(self, asset):
        sku = asset.get("tx_type_src_id").replace(f"product-", "")
        product_id = self.mage2Connector.insert_update_product(
            sku,
            asset["data"].get("attribute_set", "default"),
            asset["data"],
            asset["data"].get("type_id"),
            asset["data"].get("store_id", 0),
        )
        if len(asset["data"].get("stock_data", {})) > 0:
            self.mage2Connector.insert_update_cataloginventory_stock_item(
                sku,
                asset["data"].get("stock_data"),
                asset["data"].get("store_id", 0),
            )

        if len(asset["data"].get("category_data", [])) > 0:
            ignore_category_ids = self.setting.get("ignore_category_ids", [])
            self.mage2Connector.insert_update_categories(
                sku, asset["data"].get("category_data"), ignore_category_ids
            )
        
        self.mage2Connector.insert_update_product_tier_price(
            sku,
            asset["data"].get("tier_price_data", []),
            asset["data"].get("store_id", 0),
        )

        if asset["data"].get("variant_data"):
            self.mage2Connector.insert_update_variant(
                sku,
                asset["data"].get("variant_data"),
                asset["data"].get("store_id", 0),
            )
        self.mage2Connector.request_magento_rest_api(
            api_path="integration/products/{sku}/urlkey".format(sku=sku), method="POST"
        )
        return product_id
