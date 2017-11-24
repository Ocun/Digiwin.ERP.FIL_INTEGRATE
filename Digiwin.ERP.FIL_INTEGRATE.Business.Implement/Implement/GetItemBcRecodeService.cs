//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-14</createDate>
//<description>条码信息获取服务</description>
//---------------------------------------------------------------- 
//20170904 modi by liwei1 for P001-170717001
//20170901 modi by liwei1 for P001-170717001

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 条码信息获取服务
    /// </summary>
    [ServiceClass(typeof(IGetItemBcRecodeService))]
    [Description("条码信息获取服务")]
    public class GetItemBcRecodeService:ServiceComponent,IGetItemBcRecodeService {
        /// <summary>
        /// 根据传入的送货单单号信息获取条码信息档
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        public Hashtable GetItemBcRecode(string delivery_no) {
            try {
                //查询出退换货清单
                QueryNode queryNode = GetItemBarcodeDetail(delivery_no);
                DependencyObjectCollection itemBarcodeDetail = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组合返回值
                Hashtable result = new Hashtable{{"item_barcode_detail", itemBarcodeDetail}};
                return result;
            } catch (Exception) {
                throw;
            } 
        }

        /// <summary>
        /// 根据传入的送货单单号信息获取条码信息档
        /// </summary>
        /// <param name="deliveryNo"></param>
        /// <returns></returns>
        private QueryNode GetItemBarcodeDetail(string deliveryNo) {
            return OOQL.Select(
                                    OOQL.CreateConstants(1, GeneralDBType.Int32, "item_ver"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),//20170904 modi by liwei1 for P001-170717001 old：别名为：barcode_no
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    OOQL.CreateConstants(0m, GeneralDBType.Int32, "source_operation"),
                                    OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO", "source_no"),
                                    OOQL.CreateProperty("FIL_ARRIVAL_D.SequenceNumber", "source_seq"),
                                    OOQL.CreateConstants(1, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(1, GeneralDBType.Int32, "source_batch_seq"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"), 
                                            OOQL.CreateConstants(0m, GeneralDBType.Decimal), "barcode_qty"),
                                    OOQL.CreateConstants(1, GeneralDBType.Decimal, "print_qty"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String, "barcode_no_old"),
                                    OOQL.CreateProperty("UNIT.UNIT_CODE", "item_unit"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_VALUE"),
                                            OOQL.CreateConstants(string.Empty, GeneralDBType.String), "lot_no"),
                                    OOQL.CreateProperty("FIL_ARRIVAL_D.PACKING_QTY", "box_qty"),
                                    OOQL.CreateProperty("FIL_ARRIVAL_D.BC_TYPE", "barcode_type"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "status"))
                                .From("BC_RECORD", "BC_RECORD")
                                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID"))
                                             & (OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_ID") == OOQL.Select(
                                            OOQL.CreateProperty("BC_PROPERTY_ID"))
                                        .From("BC_PROPERTY")
                                        .Where((OOQL.CreateProperty("BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty")))))
                                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D_02")
                                .On((OOQL.CreateProperty("BC_RECORD_D_02.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID"))
                                             & (OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_ID") == OOQL.Select(
                                            OOQL.CreateProperty("BC_PROPERTY_ID"))
                                        .From("BC_PROPERTY")
                                        .Where((OOQL.CreateProperty("BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot")))))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .InnerJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID")))
                                //.InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")//20170901 mark by liwei1 for P001-170717001
                                //.On((OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid") == OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_D_ID")))//20170901 mark by liwei1 for P001-170717001
                                //20170901 add by liwei1 for P001-170717001 ---start---
                                .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                                .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                                .LeftJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                                .InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                                .On((OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO") == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"))
                                    & (OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE"))
                                    & (OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE")))
                                //20170901 add by liwei1 for P001-170717001 ---end---
                                .Where(OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo));
        }
    }
}
