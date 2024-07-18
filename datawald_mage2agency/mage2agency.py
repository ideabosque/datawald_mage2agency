# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback, json
from datawald_agency import Agency
from datawald_connector import DatawaldConnector
from mage2_connector import Mage2Connector, Mage2OrderConnector

class IgnoreException(Exception):
    pass

class Mage2Agency(Agency):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.mage2Connector = Mage2Connector(logger, **setting)
        self.mage2OrderConnector = Mage2OrderConnector(logger, **setting)
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

    def tx_transaction_tgt(self, transaction):
        return transaction

    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

    def insert_update_transactions(self, transactions):
        for transaction in transactions:
            tx_type = transaction.get("tx_type_src_id").split("-")[0]
            try:
                if tx_type == "order":
                    tgt_id = self.insert_update_order(transaction)
                else:
                    raise Exception(f"TX Type ({tx_type}) is not supported!!!")

                transaction.update(
                    {
                        "tx_status": "S",
                        "tx_note": f"datawald -> {transaction['target']}",
                        "tgt_id": tgt_id,
                    }
                )
            except IgnoreException:
                log = traceback.format_exc()
                transaction.update({"tx_status": "I", "tx_note": log, "tgt_id": "####"})
                self.logger.info(log)
            except Exception:
                log = traceback.format_exc()
                transaction.update({"tx_status": "F", "tx_note": log, "tgt_id": "####"})
                self.logger.exception(log)
        return transactions

    def insert_update_order(self, transaction):
        ecom_so = transaction["data"].get("ecom_so", None)
        warehouse = None
        order_type = transaction["data"].get("order_type", None)
        if order_type == "online":
            increment_id = ecom_so
            type = "online_order"
            if len(increment_id.split("-")) == 2:
                warehouse = increment_id.split("-")[1].lower()
                increment_id = increment_id.split("-")[0]
            # tgt_id = self.insert_update_online_order(increment_id, transaction)
        elif order_type == "offline":
            increment_id = transaction["data"].get("so_number", None)
            type = "offline_order"
            # tgt_id = self.insert_update_offline_order(increment_id, transaction)
        else:
            raise Exception(f"Undefine order type: {increment_id}")
        
        if type == "offline_order":
            if self.setting.get("ignore_offline_order", True):
                raise IgnoreException(f"Ignore offline order: {increment_id}")
            if len(self.setting.get("allow_import_offline_order_gwi_account_no", [])) > 0 and str(transaction["data"].get("customer_id")) not in self.setting.get("allow_import_offline_order_gwi_account_no", []):
                gwi_account_no = str(transaction["data"].get("customer_id"))
                raise IgnoreException(f"Ignore offline order: {increment_id}, GWI Account No. {gwi_account_no} is not allowed.")
            
        
        order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
        if type == "offline_order" and order is None:
            if self.setting.get("use_new_create_order_api", False):
                self.insert_offline_order_by_custom_api(increment_id, transaction)
            else:
                self.insert_offline_order_by_default_api(increment_id, transaction)
            self.mage2OrderConnector.adaptor.commit()

        tgt_id = self.update_mage2_order(increment_id, transaction, type, warehouse)
                
        return tgt_id

    def update_mage2_order(self, increment_id, transaction, type, warehouse=None):
        tx_type_src_id = transaction.get("tx_type_src_id")
        items = transaction["data"].get("items", [])
        if len(items) == 0 or not increment_id:
            raise Exception(f"{tx_type_src_id}: There is something wrong in data.")
        order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
        if order is None:
            raise Exception(f"{tx_type_src_id}: Can not find order")

        api_items=[]
        api_item_ids = []
        api_item_details = []

        coa_files = []
        for ns_item in items:
            if isinstance(ns_item.get("coa_file_urls", None), list) and len(ns_item.get("coa_file_urls", [])) > 0:
                for file_url in ns_item.get("coa_file_urls", []):
                    coa_files.append(file_url)
        if len(coa_files) > 0:
            self.save_coa_files(order.get("entity_id"), coa_files)
        if warehouse is not None:
            order_items = self.mage2OrderConnector.get_order_items(order.get("entity_id"))
            for ns_item in items:
                for order_item in order_items:
                    if order_item.get("parent_item_id", None) is None and order_item.get("sku") == ns_item.get("sku"):
                        product_options = json.loads(order_item.get("product_options")) if order_item.get("product_options", None) is not None else None
                        if product_options and product_options.get("info_buyRequest", {}).get("warehouse") == warehouse:
                            api_items.append({
                                "order_item_id": order_item.get("item_id"),
                                "qty": ns_item.get("qty_ordered", 0)
                            })
                            api_item_details.append(
                                {
                                    "order_item_id": order_item.get("item_id"),
                                    "sku": ns_item.get("sku"),
                                    "name": order_item.get("name"),
                                    "qty": ns_item.get("qty_ordered", 0)
                                }
                            )
                            api_item_ids.append(order_item.get("item_id"))
        if transaction["data"].get("sales_rep_name", None):
            sales_rep_name_comment = "Sales Rep: {sales_rep_name}".format(sales_rep_name=transaction["data"].get("sales_rep_name"))
            self.mage2OrderConnector.insert_order_comment(order_id=order["entity_id"], comment=sales_rep_name_comment, status=order.get("status"))

        order_ns_status = transaction["data"].get("status")
        if self.mage2OrderConnector.can_invoice_order(order) and order_ns_status == "Billed":
            can_invoice = True
            if len(api_item_ids) > 0 and not self.mage2OrderConnector.can_invoice_order_items(order, api_item_ids):
                can_invoice = False
            if can_invoice:
                append_comment = False
                comment = None
                if len(api_items) > 0:
                    append_comment = True
                    comment= "Invoiced {details}".format(
                        details=", ".join([
                            "{name}({sku})".format(name=item.get("name"), sku=item.get("sku"))
                            for item in api_item_details
                        ])
                    )
                self.mage2OrderConnector.invoice_order(
                    order_id=order.get("entity_id"),
                    capture=True,
                    items=api_items,
                    notify=False,
                    append_comment=append_comment,
                    comment=comment,
                    is_visible_on_front=False
                )
                self.mage2OrderConnector.adaptor.commit()

        if self.mage2OrderConnector.can_ship_order(order) and transaction["data"].get("fulfill_ship_status") == "_shipped" and order_ns_status == "Billed":
            can_ship = True
            if len(api_item_ids) > 0 and not self.mage2OrderConnector.can_ship_order_items(order, api_item_ids):
                can_ship = False
            
            if can_ship:
                carrier_code = transaction["data"].get("carrier_code", "Carrier")
                tracks = []
                tracking_numbers = transaction["data"].get("tracking_numbers", [])
                if len(tracking_numbers) > 0:
                    tracks = [
                        {"carrier_code": carrier_code, "title": "Tracking Number", "track_number": track_number}
                        for track_number in tracking_numbers
                    ]
                append_comment = False
                comment = None
                if len(api_items) > 0:
                    append_comment = True
                    comment= "Shipped {details}".format(
                        details=", ".join([
                            "{name}({sku})".format(name=item.get("name"), sku=item.get("sku"))
                            for item in api_item_details
                        ])
                    )
                self.mage2OrderConnector.ship_order(
                    order_id=order.get("entity_id"),
                    items=api_items,
                    notify=False, 
                    append_comment=append_comment,
                    comment=comment,
                    is_visible_on_front=False,
                    tracks=tracks,
                    ship_date=transaction["data"].get("ship_date", None)
                )
                self.mage2OrderConnector.adaptor.commit()

        transformed_status = self.transform_ns_order_status(transaction)
        if transformed_status is not None:
            ns_status = transformed_status.replace(" ","_").replace("-", "_").lower()
            if warehouse is not None:
                is_updated = self.save_warehouses_statuses(order_id=order.get("entity_id"), warehouse_code=warehouse, status=ns_status)
                if is_updated:
                    warehouse_status_comment = "Update Warehouse ({warehouse}) Status to {ns_status}".format(warehouse=warehouse, ns_status=ns_status)
                    self.mage2OrderConnector.insert_order_comment(
                        order_id=order.get("entity_id"),
                        comment=warehouse_status_comment,
                        status=order.get("status"),
                        allow_duplicate_comment=False
                    )
                if ns_status in ["canceled", "closed"] and len(api_item_ids) > 0:
                    self.mage2OrderConnector.cancel_order_items(order, api_item_ids)
                    status_comment = "Warehouse {warehouse} is canceled".format(warehouse=warehouse)
                    self.mage2OrderConnector.insert_order_comment(
                        order_id=order.get("entity_id"),
                        comment=status_comment,
                        status=order.get("status"),
                        allow_duplicate_comment=False
                    )
                return order.get("entity_id")
            order_status_state_rows = self.mage2OrderConnector.get_order_status_state()
            state_status = {}
            for row in order_status_state_rows:
                if row["state"] not in state_status:
                    state_status[row["state"]] = []
                state_status[row["state"]].append(row["status"])

            order = self.mage2OrderConnector.get_order_by_increment_id(increment_id)
            current_order_status = order.get("status")
            # ns_status = transformed_status.replace(" ","_").replace("-", "_").lower()
            current_order_state = order.get("state")
            if current_order_status == ns_status:
                return order.get("entity_id")
            
            status_comment = None
            if current_order_state == self.mage2OrderConnector.STATE_COMPLETE and ns_status not in state_status[self.mage2OrderConnector.STATE_COMPLETE]:
                status_comment = "The order is completed. Failed to update status to {ns_status}".format(ns_status=current_order_status)
            if status_comment is None:
                if current_order_state in [self.mage2OrderConnector.STATE_NEW, self.mage2OrderConnector.STATE_PROCESSING,self.mage2OrderConnector.STATE_COMPLETE,self.mage2OrderConnector.STATE_CANCELED, self.mage2OrderConnector.STATE_CLOSED]:
                    if ns_status in ["canceled", "closed"]:
                        self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), ns_status, ns_status)
                    else:
                        if current_order_state == self.mage2OrderConnector.STATE_CANCELED:
                            if ns_status == "canceled":
                                self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), current_order_state, ns_status)
                            else:
                                self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), self.mage2OrderConnector.STATE_PROCESSING, ns_status)
                        else:
                            self.mage2OrderConnector.update_order_state_status(order.get("entity_id"), current_order_state, ns_status)
            
                status_comment = "Update Status to {ns_status}".format(ns_status=ns_status)
            if status_comment is not None:
                self.mage2OrderConnector.insert_order_comment(
                    order_id=order.get("entity_id"),
                    comment=status_comment,
                    status=ns_status,
                    allow_duplicate_comment=True
                )
        return order.get("entity_id")
                    
        
    def transform_ns_order_status(self, transaction):
        status = None
        if transaction["data"].get("status") in ["canceled", "Cancelled"]:
            status = "canceled"
        elif transaction["data"].get("status") in ["closed", "Closed"]:
            status = "closed"
        else:
            if transaction["data"].get("hold_reason"):
                status = transaction["data"].get("hold_reason")
            if transaction["data"].get("fulfill_ship_status"):
                if_status = transaction["data"].get("fulfill_ship_status")
                suffix = ""
                if transaction["data"].get("carrier_code") == "CUSTOMER PICKUP":
                    suffix = "_for_cusotmer"
                status = "{if_status}{suffix}".format(if_status=if_status, suffix=suffix)
        return status
    
    def insert_offline_order_by_default_api(self, increment_id, transaction):
        tx_type_src_id = transaction.get("tx_type_src_id")
        items = transaction["data"].get("items", [])
        if len(items) == 0 or not increment_id:
            raise Exception(f"{tx_type_src_id}: No items or empty increment_id")
        if not transaction["data"].get("customer_id"):
            raise Exception(f"{tx_type_src_id}: Empty customer_id")
        customer_id = self.get_customer_id_by_company_no(transaction["data"].get("customer_id"))
        if customer_id is None:
            raise Exception(f"{tx_type_src_id}: Can not find customer by company NO in Magento")
        customer_data = self.get_customer_data(customer_id)
        avaliable_items = []
        subtotal = 0
        shipping_amount = float(transaction["data"].get("shipping_amount", 0))
        total_qty_ordered = 0
        for item in items:
            if self.is_shipping_charge_sku(item.get("sku")):
                shipping_amount = shipping_amount + + float(item.get("row_total"))
                continue
            product_id = self.mage2Connector.get_product_id_by_sku(item.get("sku"))
            if product_id != 0:
                type_id = self.mage2Connector.get_product_type_id_by_sku(item.get("sku"))
                product_name = self.mage2Connector.get_entity_attribute_value("catalog_product", product_id, "name", store_id=0)
                weight = self.mage2Connector.get_entity_attribute_value("catalog_product", product_id, "weight", store_id=0)
                if weight is None:
                    weight = 0.001
                avaliable_items.append(dict(item, **{"product_id": product_id, "product_type": type_id, "product_name": product_name, "weight": weight}))
                subtotal = subtotal + float(item.get("row_total"))
                total_qty_ordered = total_qty_ordered + float(item.get("qty_ordered", 0))
            else:
                raise Exception(f"{tx_type_src_id}: Sku: '{item.get('sku')}' not in Magento")
        if len(avaliable_items) == 0:
            raise Exception(f"{tx_type_src_id}: No avaliable product items")
        grand_total = subtotal + shipping_amount
        billing_address = transaction["data"].get("billing_address", {})
        shipping_address = transaction["data"].get("shipping_address", {})
        posts = {
            "entity": {
                "state": "new",
                "status": "pending",
                "is_virtual": 0,
                "store_id": 1,
                "customer_id": customer_id,
                "base_grand_total": grand_total,
                "base_subtotal": subtotal,
                "grand_total": grand_total,
                "subtotal": subtotal,
                "shipping_amount": shipping_amount,
                "base_shipping_amount": shipping_amount,
                "total_qty_ordered": total_qty_ordered,
                "base_currency_code": "USD",
                "customer_email": customer_data["email"],
                "customer_firstname": customer_data["firstname"],
                "customer_lastname": customer_data["lastname"],
                "shipping_description": transaction["data"].get("carrier_code"),
                "store_currency_code": "USD",
                "order_currency_code": "USD",
                "global_currency_code": "USD",
                "increment_id": increment_id,
                "billing_address": {
                    "address_type": "billing",
                    "region": billing_address.get("region"),
                    "city": billing_address.get("city"),
                    "street": [billing_address.get("street")],
                    "postcode": billing_address.get("postcode"),
                    "firstname": billing_address.get("contact").split(" ")[0] if billing_address.get("contact") is not None else "",
                    "lastname": (" ".join(billing_address.get("contact").split(" ")[1:]) if len(billing_address.get("contact").split(" ")) > 1 else "") if billing_address.get("contact") is not None else "",
                    "company": billing_address.get("company"),
                    "country_id": billing_address.get("country_id"),
                    "telephone": billing_address.get("telephone", "######")
                },
                "items": [
                ],
                "payment": {
                    "method": self.setting.get("offline_default_payment_method", "checkmo")
                },
                "extension_attributes": {
                    "amasty_order_attributes": [
                        {
                            "attribute_code": "is_offline_order",
                            "value": True
                        }
                    ], 
                    "shipping_assignments": [
                        {
                            "shipping": {
                                "address": {
                                    "address_type": "shipping",
                                    "region": shipping_address.get("region"),
                                    "city": shipping_address.get("city"),
                                    "street": [shipping_address.get("street")],
                                    "postcode": shipping_address.get("postcode"),
                                    "firstname": shipping_address.get("contact").split(" ")[0] if shipping_address.get("contact") is not None else "",
                                    "lastname": (" ".join(shipping_address.get("contact").split(" ")[1:]) if len(shipping_address.get("contact").split(" ")) > 1 else "") if shipping_address.get("contact") is not None else "",
                                    "company": shipping_address.get("company"),
                                    "country_id": shipping_address.get("country_id"),
                                    "telephone": shipping_address.get("telephone", "######")
                                },
                                "method": self.setting.get("offline_default_shipping_method", "freeshipping_freeshipping")
                            }
                        }
                    ]
                },
                "created_at": transaction["data"].get("created_at")
            }
        }
        for avaliable_item in avaliable_items:
            posts["entity"]["items"].append(
                {
                    "sku": avaliable_item.get("sku"),
                    "product_id": avaliable_item.get("product_id"),
                    "product_type": avaliable_item.get("product_type"),
                    "name": avaliable_item.get("product_name"),
                    "weight": str(avaliable_item.get("weight", 0)),
                    "store_id": 1,
                    "is_virtual": 0,
                    "qty_ordered": avaliable_item.get("qty_ordered", 0),
                    "price": avaliable_item.get("price", 0),
                    "base_price": avaliable_item.get("price", 0),
                    "row_total": avaliable_item.get("row_total", 0),
                    "base_row_total": avaliable_item.get("row_total", 0)
                }
            )
            
        response = self.mage2OrderConnector.request_magento_rest_api(
            api_path="orders/create", method="PUT", payload=posts
        )
    
    def is_shipping_charge_sku(self, sku):
        shipping_charge_skus = self.setting.get("shipping_charge_skus", [])
        if not isinstance(shipping_charge_skus, list) or len(shipping_charge_skus) == 0:
            return False
        if sku in shipping_charge_skus:
            return True
        return False
        
    def insert_offline_order_by_custom_api(self, increment_id, transaction):
        tx_type_src_id = transaction.get("tx_type_src_id")
        items = transaction["data"].get("items", [])
        if len(items) == 0 or not increment_id:
            raise Exception(f"{tx_type_src_id}: No items or empty increment_id")
        if not transaction["data"].get("customer_id"):
            raise Exception(f"{tx_type_src_id}: Empty customer_id")
        company_no = transaction["data"].get("customer_id")
        avaliable_items = []
        for item in items:
            # move product check logic to Magento
            # product_id = self.mage2Connector.get_product_id_by_sku(item.get("sku"))
            # if product_id != 0:
            avaliable_items.append(item)
        if len(avaliable_items) == 0:
            raise Exception(f"{tx_type_src_id}: No avaliable product items")
        warehouse_code_mapping = self.setting.get("location_name_warehouse_code_mapping", {})
        warehouse_code = warehouse_code_mapping.get(transaction["data"].get("location_name"), "chino")
        offline_default_shipping_method = self.setting.get("offline_default_shipping_method", "freeshipping_freeshipping")
        carrier_code, method_code = (offline_default_shipping_method.split("_")[0], offline_default_shipping_method.split("_")[1])
        warehouse_data = {
            "warehouse_code": warehouse_code,
            "items": [
                {
                    "sku": item.get("sku"),
                    "qty_ordered": item.get("qty_ordered", 0),
                    "price": item.get("price", 0),
                    "row_total": item.get("row_total", 0)
                }
                for item in avaliable_items
            ],
            "shipping_method": {
                "carrier_code": carrier_code,
                "method_code": method_code,
                "method_name": transaction["data"].get("carrier_code", "Will Call"),
                "amount": transaction["data"].get("shipping_amount", 0)
            }
        }
        billing_address = transaction["data"].get("billing_address", {})
        shipping_address = transaction["data"].get("shipping_address", {})
        customer_po = transaction["data"].get("customer_po", None)
        posts = {
            "order": {
                "order_type": "offline_ns",
                "integration_id": increment_id,
                "customer_id": company_no,
                "increment_id": increment_id,
                "customer_po": customer_po,
                "billing_address": {
                    "region": billing_address.get("region"),
                    "city": billing_address.get("city"),
                    "street": [billing_address.get("street")],
                    "postcode": billing_address.get("postcode"),
                    "firstname": billing_address.get("contact").split(" ")[0] if billing_address.get("contact") is not None else "",
                    "lastname": (" ".join(billing_address.get("contact").split(" ")[1:]) if len(billing_address.get("contact").split(" ")) > 1 else "") if billing_address.get("contact") is not None else "",
                    "company": billing_address.get("company"),
                    "country_id": billing_address.get("country_id"),
                    "telephone": billing_address.get("telephone", "######")
                },
                "shipping_address": {
                    "region": shipping_address.get("region"),
                    "city": shipping_address.get("city"),
                    "street": [shipping_address.get("street")],
                    "postcode": shipping_address.get("postcode"),
                    "firstname": shipping_address.get("contact").split(" ")[0] if shipping_address.get("contact") is not None else "",
                    "lastname": (" ".join(shipping_address.get("contact").split(" ")[1:]) if len(shipping_address.get("contact").split(" ")) > 1 else "") if shipping_address.get("contact") is not None else "",
                    "company": shipping_address.get("company"),
                    "country_id": shipping_address.get("country_id"),
                    "telephone": shipping_address.get("telephone", "######")
                },
                "warehouses_data": [warehouse_data],
                "created_at": transaction["data"].get("created_at")
            }
        }
        response = self.mage2OrderConnector.request_magento_rest_api(
            api_path="integration/orders/create", method="PUT", payload=posts
        )

    def get_customer_id_by_company_no(self, company_no):
        try:
            (data_type, attribute_metadata) = self.mage2Connector.get_attribute_metadata("company_no", "customer")
        except Exception as e:
            return None
        attribute_id = attribute_metadata["attribute_id"]
        self.mage2Connector.adaptor.mysql_cursor.execute(
            "SELECT * FROM customer_entity_varchar WHERE attribute_id = %s AND value = %s;",
            [attribute_id, company_no]
        )
        res = self.mage2Connector.adaptor.mysql_cursor.fetchone()
        if res is None:
            return None
        else:
            return res["entity_id"]

    def get_customer_data(self, customer_id):
        self.mage2Connector.adaptor.mysql_cursor.execute(
            "SELECT * FROM customer_entity WHERE entity_id = %s;",
            [customer_id]
        )
        res = self.mage2Connector.adaptor.mysql_cursor.fetchone()
        return res
    
    def save_warehouses_statuses(self, order_id, warehouse_code, status):
        try:
            warehouses_statuses = {}
            (entity_id, origin_value) = self.get_order_attribute_value(order_id=order_id, attribute_code="warehouse_statuses")
            if origin_value is not None:
                warehouses_statuses = json.loads(origin_value)
            warehouses_statuses.update({
                warehouse_code: status
            })
            warehouses_statuses_string = json.dumps(warehouses_statuses)
            if origin_value == warehouses_statuses_string:
                return False
            self.save_order_attribute_value(order_id=order_id, attribute_code="warehouse_statuses", value=warehouses_statuses_string, entity_id=entity_id)
            return True
        except Exception as e:
            return False

    def get_order_attribute_value(self, order_id, attribute_code):
        (data_type, attribute_metadata) = self.mage2Connector.get_attribute_metadata(attribute_code, "amasty_checkout")
        attribute_id = attribute_metadata["attribute_id"]
        attribute_value_table = "amasty_order_attribute_entity_{data_type}".format(data_type=data_type)
        sql = """
            SELECT e.entity_id, v.value
            FROM amasty_order_attribute_entity AS e
            LEFT JOIN {attribute_value_table} as v ON v.entity_id = e.entity_id
            WHERE v.attribute_id = %s AND e.parent_id = %s AND e.parent_entity_type = 1;
        """.format(attribute_value_table=attribute_value_table)

        self.mage2Connector.adaptor.mysql_cursor.execute(
            sql,
            [
                attribute_id,
                order_id
            ]
        )
        res = self.mage2Connector.adaptor.mysql_cursor.fetchone()
        value = None
        entity_id = None
        if res is not None:
            value = res["value"]
            entity_id = res["entity_id"]
        return (entity_id, value)
    
    def save_order_attribute_value(self, order_id, attribute_code, value, entity_id=None):
        try:
            (data_type, attribute_metadata) = self.mage2Connector.get_attribute_metadata(attribute_code, "amasty_checkout")
        except Exception as e:
            return
        if entity_id is None:
            self.mage2Connector.adaptor.mysql_cursor.execute(
                """
                SELECT entity_id 
                FROM amasty_order_attribute_entity
                WHERE parent_id = %s and parent_entity_type = 1;
                """,
                [
                    order_id
                ]
            )
            res = self.mage2Connector.adaptor.mysql_cursor.fetchone()
            if res is not None:
                entity_id = res["entity_id"]

        if entity_id is None:
            return
        attribute_id = attribute_metadata["attribute_id"]
        attribute_value_table = "amasty_order_attribute_entity_{data_type}".format(data_type=data_type)
        sql = """
            INSERT INTO {attribute_value_table} (`attribute_id`, `entity_id`, `value`)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE `value`=%s;
        """.format(attribute_value_table=attribute_value_table)
        self.mage2Connector.adaptor.mysql_cursor.execute(
            sql,
            [
                attribute_id,
                entity_id,
                value,
                value
            ]
        )
        self.mage2Connector.adaptor.commit()

    def save_coa_files(self, order_id, coa_files=[]):
        if len(coa_files) == 0:
            return
        try:
            (data_type, attribute_metadata) = self.mage2Connector.get_attribute_metadata("coa_file_urls", "amasty_checkout")
        except Exception as e:
            return
        try:
            coa_string = "\n".join(coa_files)
            attribute_id = attribute_metadata["attribute_id"]
            self.mage2Connector.adaptor.mysql_cursor.execute(
                """
                INSERT INTO `amasty_order_attribute_entity_text` (`attribute_id`, `entity_id`, `value`)
                    SELECT %s as attribute_id, a.entity_id as entity_id, %s as value
                    FROM `amasty_order_attribute_entity` a
                    where a.parent_id = %s and a.parent_entity_type = 1
                ON DUPLICATE KEY UPDATE `value`=%s;
                """,
                [
                    attribute_id,
                    coa_string,
                    order_id,
                    coa_string
                ]
            )
            self.mage2Connector.adaptor.commit()
        except Exception as e:
            self.logger.error(str(e))