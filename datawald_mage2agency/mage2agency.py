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

    def tx_asset_tgt(self, asset):
        return asset

    def tx_asset_tgt_ext(self, new_asset, asset):
        pass

    def insert_update_assets(self, assets):
        for asset in assets:
            tx_type = asset.get("tx_type_src_id").split("-")[0]
            sku = asset.get("tx_type_src_id").replace(f"{tx_type}-", "")
            try:
                if tx_type == "product":
                    self.mage2Connector.insert_update_product(
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
                        self.mage2Connector.insert_update_categories(
                            sku, asset["data"].get("category_data")
                        )

                    if len(asset["data"].get("tier_price_data", [])) > 0:
                        self.mage2Connector.insert_update_product_tier_price(
                            sku,
                            asset["data"].get("tier_price_data"),
                            asset["data"].get("store_id", 0),
                        )

                    if asset["data"].get("variant_data"):
                        self.mage2Connector.insert_update_variant(
                            sku,
                            asset["data"].get("variant_data"),
                            asset["data"].get("store_id", 0),
                        )

                else:
                    raise Exception(f"TX Type ({tx_type}) is not supported!!!")
            except Exception:
                log = traceback.format_exc()
                # asset.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(log)
        return assets
