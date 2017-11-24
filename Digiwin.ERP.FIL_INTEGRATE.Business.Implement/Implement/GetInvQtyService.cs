//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/14 13:49:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>根据提供的工厂、品号等信息，获取对应的现有库存量</description>
//20170329 modi by wangrm for P001-170316001
//20170515 modi by liwei1 for P001-170420001

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetInvQtyService))]
    [Description("根据提供的工厂、品号等信息，获取对应的现有库存量")]
    public sealed class GetInvQtyService : ServiceComponent, IGetInvQtyService {
        #region 相关服务

        private IInfoEncodeContainer _encodeSrv;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer EncodeSrv {
            get {
                if (_encodeSrv == null)
                    _encodeSrv = GetService<IInfoEncodeContainer>();

                return _encodeSrv;
            }
        }

        #endregion

        #region IGetInvQtyService 成员
        //20170515 add by liwei1 for P001-170420001 ---begin---
        /// <summary>
        /// 根据提供的工厂、品号等信息，获取对应的现有库存量
        /// </summary>
        /// <param name="site_no">工厂</param>
        /// <param name="show_zero_inventory">显示零库存</param>
        /// <param name="scan_warehouse_no">扫描仓库</param>
        /// <param name="scan_storage_spaces_no">扫描储位</param>
        /// <returns></returns>
        public Hashtable GetInvQty(string site_no, string show_zero_inventory, string scan_warehouse_no,
            string scan_storage_spaces_no){
            return GetInvQty(site_no, show_zero_inventory, string.Empty, scan_warehouse_no, scan_storage_spaces_no, null);
        }
        //20170515 add by liwei1 for P001-170420001 ---end---

        /// <summary>
        /// 根据提供的工厂、品号等信息，获取对应的现有库存量
        /// </summary>
        /// <param name="site_no">工厂</param>
        /// <param name="show_zero_inventory">显示零库存</param>
        /// <param name="scan_barcode">扫描条形码</param>
        /// <param name="scan_warehouse_no">扫描仓库</param>
        /// <param name="scan_storage_spaces_no">扫描储位</param>
        /// <returns></returns>
        public Hashtable GetInvQty(string site_no, string show_zero_inventory, string scan_barcode, string scan_warehouse_no,
            string scan_storage_spaces_no) {
            //20170329 add by wangrm for P001-170316001======start=======
            return GetInvQty(site_no, show_zero_inventory, scan_barcode, scan_warehouse_no, scan_storage_spaces_no, null);
        }

        public Hashtable GetInvQty(string site_no, string show_zero_inventory, string scan_barcode, string scan_warehouse_no,
            string scan_storage_spaces_no, string program_job_no) {

            //20170329 add by wangrm for P001-170316001======end=======
            Hashtable result = new Hashtable();  //返回值

            #region 参数检查
            //if (Maths.IsEmpty(scan_barcode)) {//20170331 mark by wangyq for P001-170327001
            //    throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "scan_barcode" }));
            //}
            if (Maths.IsEmpty(site_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "site_no" }));
            }
            //20170329 add by wangrm for P001-170316001======start=======
            if (program_job_no != null && Maths.IsEmpty(program_job_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "program_job_no" }));
            }
            //20170329 add by wangrm for P001-170316001======end=======
            #endregion
            if (program_job_no == null || program_job_no == "15") {
                //属性值
                object propertyId = QueryItemLot();
                if (propertyId == null)
                    throw new BusinessRuleException("QueryItemLot");

                //品号批号相关信息
                //20170331 modi by wangyq for P001-170327001  ==================begin==================
                object itemId = Maths.GuidDefaultValue();
                object itemFeatureId = Maths.GuidDefaultValue();
                string itemLotCode = string.Empty;
                if (!string.IsNullOrEmpty(scan_barcode)) {
                    DependencyObject itemInfo = QueryBcBarCode(propertyId, scan_barcode);
                    if (itemInfo != null) {
                        itemId = itemInfo["ITEM_ID"];
                        itemFeatureId = itemInfo["ITEM_FEATURE_ID"];
                        itemLotCode = itemInfo["ITEM_LOT_CODE"].ToStringExtension();
                    }
                }
                //DependencyObject itemInfo = QueryBcBarCode(propertyID, scan_barcode);
                //if (itemInfo == null)
                //    throw new BusinessRuleException("Query_BC_RECORD");
                //20170331 modi by wangyq for P001-170327001  ==================end==================
                //查品号库存
                DependencyObjectCollection itemInvQty = QueryItemInvQty(itemId, itemFeatureId, itemLotCode, site_no, scan_warehouse_no, scan_storage_spaces_no, show_zero_inventory);
                //20170331 modi by wangyq for P001-170327001 old:
                //DependencyObjectCollection itemInvQTY = QueryItemInvQty(itemInfo, site_no, scan_warehouse_no, scan_storage_spaces_no, show_zero_inventory);
                result.Add("item_detail", itemInvQty);

                //查条码库存
                bool bcLintFlag = IsBcLineManagement();
                if (!bcLintFlag) {
                    DependencyObjectCollection emptyColl = CreateReturnCollection();
                    result.Add("barcode_detail", emptyColl);
                } else {
                    DependencyObjectCollection barCodeInvQty = QueryBarCodeInvQty(scan_barcode, site_no, scan_warehouse_no, scan_storage_spaces_no, show_zero_inventory);
                    result.Add("barcode_detail", barCodeInvQty);
                }
                //20170329 add by wangrm for P001-170316001======start=======
            } else if (program_job_no == "18") {
                DependencyObjectCollection emptyItemColl = CreateReturnCollectionForItemDetail();
                result.Add("item_detail", emptyItemColl);

                bool bcLintFlag = IsBcLineManagement();
                if (!bcLintFlag) {
                    DependencyObjectCollection emptyColl = CreateReturnCollection();
                    result.Add("barcode_detail", emptyColl);
                } else {
                    DependencyObjectCollection barCodeInvQty = QueryBcFrozen(scan_barcode);
                    result.Add("barcode_detail", barCodeInvQty);
                }
            }
            //20170329 add by wangrm for P001-170316001======end=======
            return result;
        }

        #endregion

        #region 自定义方法

        /// <summary>
        /// 创建空的服务返回集合
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("barcode_detail");
            type.RegisterSimpleProperty("barcode_no", typeof(string));
            type.RegisterSimpleProperty("item_no", typeof(string));
            type.RegisterSimpleProperty("item_feature_no", typeof(string));//20170329 add by wangrm for P001-170316001
            type.RegisterSimpleProperty("item_feature_name", typeof(string));//20170329 add by wangrm for P001-170316001
            type.RegisterSimpleProperty("warehouse_no", typeof(string));
            type.RegisterSimpleProperty("storage_spaces_no", typeof(string));
            type.RegisterSimpleProperty("lot_no", typeof(string));
            type.RegisterSimpleProperty("inventory_unit", typeof(string));
            type.RegisterSimpleProperty("inventory_qty", typeof(decimal));

            DependencyObjectCollection rtn = new DependencyObjectCollection(type);
            return rtn;
        }

        /// <summary>
        /// 创建空的服务返回集合
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateReturnCollectionForItemDetail() {
            DependencyObjectType type = new DependencyObjectType("item_detail");
            type.RegisterSimpleProperty("item_no", typeof(string));
            type.RegisterSimpleProperty("item_name", typeof(string));
            type.RegisterSimpleProperty("item_spec", typeof(string));
            type.RegisterSimpleProperty("warehouse_no", typeof(string));
            type.RegisterSimpleProperty("item_feature_no", typeof(string));
            type.RegisterSimpleProperty("item_feature_name", typeof(string));
            type.RegisterSimpleProperty("storage_spaces_no", typeof(string));
            type.RegisterSimpleProperty("lot_no", typeof(string));
            type.RegisterSimpleProperty("inventory_unit", typeof(string));
            type.RegisterSimpleProperty("inventory_qty", typeof(decimal));

            DependencyObjectCollection rtn = new DependencyObjectCollection(type);

            return rtn;
        }

        public object QueryItemLot() {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("BC_PROPERTY.BC_PROPERTY_ID"))
                .From("BC_PROPERTY", "BC_PROPERTY")
                .Where(OOQL.AuthFilter("BC_PROPERTY", "BC_PROPERTY")
                    & OOQL.CreateProperty("BC_PROPERTY.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot"));
            return GetService<IQueryService>().ExecuteScalar(node);
        }

        public DependencyObject QueryBcBarCode(object propertyId, string barCodeNo) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("BC_RECORD.ITEM_ID")  //品号
                    , OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")  //特征码
                    , Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"), OOQL.CreateConstants(""), "ITEM_LOT_CODE")  //批号
                    )
                .From("BC_RECORD", "BC_RECORD")
                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                .On(OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID")
                    & OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_ID") == OOQL.CreateConstants(propertyId))
                .Where(OOQL.AuthFilter("BC_RECORD", "BC_RECORD")
                    & OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barCodeNo));

            DependencyObjectCollection coll = GetService<IQueryService>().ExecuteDependencyObject(node);
            if (coll.Count > 0)
                return coll[0];

            return null;
        }

        public DependencyObjectCollection QueryItemInvQty(object itemId, object itemFeatureId, string itemLotCode, string siteNo, string scanWarehouseNo,//20170331 modi by wangyq for P001-170327001 old:public DependencyObjectCollection QueryItemInvQty(DependencyObject itemInfo, string siteNo, string scan_warehouse_no,
            string scanStorageSpacesNo, string showZeroInventory) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE", "item_feature_no"),//wangrm
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "item_feature_name"),//wangrm
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(""), "warehouse_no"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(""), "storage_spaces_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(""), "lot_no"),
                    Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(""), "inventory_unit"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY")), OOQL.CreateConstants(0), "inventory_qty")
                )
                .From("ITEM_WAREHOUSE_BIN", "ITEM_WAREHOUSE_BIN")
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                .LeftJoin("PLANT")
                .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .Where(OOQL.AuthFilter("ITEM_WAREHOUSE_BIN", "ITEM_WAREHOUSE_BIN")
                    & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                        & (OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_ID") == OOQL.CreateConstants(itemId)//20170331 modi by wangyq for P001-170327001 old:itemInfo["ITEM_ID"]
                            | (OOQL.CreateConstants(itemId) == OOQL.CreateConstants(Maths.GuidDefaultValue())))//20170331 modi by wangyq for P001-170327001 old:itemInfo["ITEM_ID"]
                        & (OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_FEATURE_ID") == OOQL.CreateConstants(itemFeatureId)//20170331 modi by wangyq for P001-170327001 old:itemInfo["ITEM_FEATURE_ID"]
                            | (OOQL.CreateConstants(itemFeatureId) == OOQL.CreateConstants(Maths.GuidDefaultValue())))//20170331 modi by wangyq for P001-170327001 old:itemInfo["ITEM_FEATURE_ID"]
                        & (OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(scanWarehouseNo)
                            | (OOQL.CreateConstants(scanWarehouseNo) == OOQL.CreateConstants("")))
                        & (OOQL.CreateProperty("BIN.BIN_CODE") == OOQL.CreateConstants(scanStorageSpacesNo)
                            | (OOQL.CreateConstants(scanStorageSpacesNo) == OOQL.CreateConstants("")))
                        & (OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateConstants(itemLotCode)//20170331 modi by wangyq for P001-170327001 old:itemInfo["ITEM_LOT_CODE"]
                            | (OOQL.CreateConstants(itemLotCode) == OOQL.CreateConstants("")))//20170331 modi by wangyq for P001-170327001 old:itemInfo["ITEM_LOT_CODE"]
                    ))
                .GroupBy(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),//wangrm
                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                    OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE")
                )
                .Having((OOQL.CreateConstants(showZeroInventory) == OOQL.CreateConstants("N")
                            & Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY")), OOQL.CreateConstants(0)) != OOQL.CreateConstants(0))
                        | (OOQL.CreateConstants(showZeroInventory) == OOQL.CreateConstants("Y"))
                );
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        public DependencyObjectCollection QueryBarCodeInvQty(string barCodeNo, string siteNo, string scanWarehouseNo,
            string scanStorageSpacesNo, string showZeroInventory) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO", "barcode_no"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),//20170329 add by wangrm for P001-170316001
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),//20170329 add by wangrm for P001-170316001
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE", "item_feature_no"),//20170329 add by wangrm for P001-170316001
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "item_feature_name"),//20170329 add by wangrm for P001-170316001
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(""), "warehouse_no"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(""), "storage_spaces_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(""), "lot_no"),
                    Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(""), "inventory_unit"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("BC_INVENTORY.QTY")), OOQL.CreateConstants(0), "inventory_qty")
                )
                .From("BC_INVENTORY", "BC_INVENTORY")
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("BC_INVENTORY.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("BC_INVENTORY.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                .LeftJoin("PLANT")
                .On(OOQL.CreateProperty("BC_INVENTORY.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .Where(OOQL.AuthFilter("BC_INVENTORY", "BC_INVENTORY")
                    & ((OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO") == OOQL.CreateConstants(barCodeNo) |
                                OOQL.CreateConstants(barCodeNo) == OOQL.CreateConstants(string.Empty)) //20170331 add  by wangyq for P001-170327001
                        & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                        & (OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(scanWarehouseNo)
                            | (OOQL.CreateConstants(scanWarehouseNo) == OOQL.CreateConstants("")))
                        & (OOQL.CreateProperty("BIN.BIN_CODE") == OOQL.CreateConstants(scanStorageSpacesNo)
                            | (OOQL.CreateConstants(scanStorageSpacesNo) == OOQL.CreateConstants("")))
                    ))
                .GroupBy(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),//wangrm
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),//wangrm
                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                    OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE")

                )
                .Having((OOQL.CreateConstants(showZeroInventory) == OOQL.CreateConstants("N")
                            & Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("BC_INVENTORY.QTY")), OOQL.CreateConstants(0)) != OOQL.CreateConstants(0))
                        | (OOQL.CreateConstants(showZeroInventory) == OOQL.CreateConstants("Y"))
                );
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        public bool IsBcLineManagement() {
            QueryNode node = OOQL.Select("PARA_FIL.BC_INVENTORY_MANAGEMENT")
                .From("PARA_FIL", "PARA_FIL")
                .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));

            object obj = GetService<IQueryService>().ExecuteScalar(node);
            if (obj == null)
                return false;

            return obj.ToBoolean();
        }

        //20170329 add by wangrm for P001-170316001======start=======
        private DependencyObjectCollection QueryBcFrozen(string scanBarcode) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                       OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                       OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                       OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                       OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE", "item_feature_no"),
                                       OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "item_feature_name"),
                                       Formulas.Case(null, OOQL.CreateConstants("N"), OOQL.CreateCaseArray(
                                                                 OOQL.CreateCaseItem(((OOQL.CreateProperty("BC_RECORD.FROZEN_STATUS") != OOQL.CreateEmptyConstants())
                                                                               & (OOQL.CreateProperty("BC_RECORD.FROZEN_STATUS") != OOQL.CreateNullConstant())),
                                                                              OOQL.CreateProperty("BC_RECORD.FROZEN_STATUS"))), "frozen"),
                                       OOQL.CreateConstants(string.Empty, "warehouse_no"),
                                       OOQL.CreateConstants(string.Empty, "storage_spaces_no"),
                                       OOQL.CreateConstants(string.Empty, "lot_no"),
                                       OOQL.CreateConstants(0m, "inventory_qty"),
                                       OOQL.CreateConstants(string.Empty, "inventory_unit"))
                               .From("BC_RECORD", "BC_RECORD")
                               .LeftJoin("ITEM", "ITEM")
                               .On(OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                               .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                               .On(OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                               .Where(OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(scanBarcode));
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }
        //20170329 add by wangrm for P001-170316001======end=======
        #endregion
    }
}
