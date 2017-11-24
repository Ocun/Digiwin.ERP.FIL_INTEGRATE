//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-13</createDate>
//<description>建立送货单服务</description>
//---------------------------------------------------------------- 
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IInsertFilArrivalService))]
    [Description("建立送货单服务")]
    public class InsertFilArrivalService : ServiceComponent, IInsertFilArrivalService {
        /// <summary>
        /// 根据传入的信息建立送货单
        /// </summary>
        /// <param name="site_no">门店</param>
        /// <param name="delivery_no">送货单号</param>
        /// <param name="create_date">单据日期</param>
        /// <param name="supplier_no">供应商编号</param>
        /// <param name="purchase_type">采购性质</param>
        /// <param name="receipt_address">收货地址</param>
        /// <param name="receipt_no">收货单号</param>
        /// <param name="print_qty">列印次数</param>
        /// <param name="remark">备注</param>
        /// <param name="status">状态</param>
        /// <param name="delivery_detail">单身数据集</param>
        public void InsertFilArrival(string site_no, string delivery_no, string create_date, string supplier_no, string purchase_type, string receipt_address,
            string receipt_no, string print_qty, string remark, string status, DependencyObjectCollection delivery_detail) {
            try {
                //生成主键服务 
                IPrimaryKeyService keyService = this.GetServiceForThisTypeKey<IPrimaryKeyService>();
                object filArrivalId= keyService.CreateId();//主键
                //送货单
                DataTable filArrival = FilArrivalNewDt(filArrivalId,site_no, delivery_no, create_date, supplier_no,
                    purchase_type, receipt_address, receipt_no, print_qty, remark, status);

                //送货单单身
                DataTable filArrivalD = FilArrivalDNewDt(delivery_detail,site_no,delivery_no,supplier_no, filArrivalId, keyService);

                //获取需要拆入数据表的ColumnMappings
                List<BulkCopyColumnMapping> mapping = GetBulkCopyColumnMapping(filArrival.Columns);
                //获取需要拆入数据表的ColumnMappings
                List<BulkCopyColumnMapping> mappingD = GetBulkCopyColumnMapping(filArrivalD.Columns);

                using (ITransactionService transActionService = GetService<ITransactionService>()) {
                    //插入送货单数据
                    GetService<IQueryService>().BulkCopy(filArrival, filArrival.TableName, mapping.ToArray());
                    //插入送货单单身数据
                    GetService<IQueryService>().BulkCopy(filArrivalD, filArrivalD.TableName, mappingD.ToArray());
                    transActionService.Complete();
                }
            } catch (Exception) {
                throw;
            }
            
        }

        /// <summary>
        /// 创建送货单临时表
        /// </summary>
        /// <returns></returns>
        private DataTable FilArrivalNewDt(object filArrivalId,string siteNo, string deliveryNo, string createDate, string supplierNo,
            string purchaseType, string receiptAddress, string receiptNo, string printQty, string remark, string status) {
            //创建临时表
            DataTable dataTable = CreateFilArrivalInfo();
            ILogOnService loginSrv = GetService<ILogOnService>();
            //新增行
            DataRow dr = dataTable.NewRow();
            dr["FIL_ARRIVAL_ID"] = filArrivalId;        //主键
            dr["BOOK_CODE"] = "99";     //企业编号
            dr["PLANT_CODE"] = siteNo;   //门店
            dr["DOC_NO"] = deliveryNo;     //发货单号
            dr["DOC_DATE"] = createDate.ToDate();      //单据日期
            dr["COMPANY_CODE"] = supplierNo;        //公司代号
            dr["CATEGORY"] = purchaseType;      //采购性质
            dr["ADDR_NAME"] = receiptAddress;       //收货地址
            dr["ARRIVAL_NO"] = receiptNo;       //收货单号
            dr["PRINT"] = printQty.ToInt32();     //打印次数
            dr["REMARK"] = remark;      //备注
            dr["STATUS"] = status;       //状态
            //系统管理字段
            dr["CreateBy"] = loginSrv.CurrentUserId;
            dr["CreateDate"] = DateTime.Now;
            dr["ModifiedBy"] = loginSrv.CurrentUserId;
            dr["ModifiedDate"] = DateTime.Now;
            dr["LastModifiedBy"] = loginSrv.CurrentUserId;
            dr["LastModifiedDate"] = DateTime.Now;
            //添加至dataTable中
            dataTable.Rows.Add(dr);
            return dataTable;
        }

        /// <summary>
        /// 创建送货单单身临时表
        /// </summary>
        /// <returns></returns>
        private DataTable FilArrivalDNewDt(DependencyObjectCollection deliveryDetail,string siteNo,string deliveryNo,string supplierNo,object parentId, IPrimaryKeyService keyService) {
            //创建临时表
            DataTable dataTable = CreateFilArrivalDInfo();
            ILogOnService loginSrv = GetService<ILogOnService>();
            foreach (DependencyObject item in deliveryDetail) {
                //新增行
                DataRow dr = dataTable.NewRow();
                dr["FIL_ARRIVAL_D_ID"] = keyService.CreateId();//主键
                dr["ParentId"] = parentId;  //父主键
                dr["BOOK_CODE"] = "99";     //企业编号
                dr["PLANT_CODE"] = siteNo;	//门店
                dr["DOC_NO"] = deliveryNo;	//发货单号
                dr["SequenceNumber"] = item["seq"];	//项次
                dr["COMPANY_CODE"] = supplierNo;	//公司代号
                dr["ORDER_NO"] = item["purchase_no"];	//采购单号
                dr["ORDER_SE"] = item["purchase_seq"].ToInt32();	//采购单序号
                dr["ORDER_SE_SE"] = item["line_seq"].ToInt32();	//采购单子序号
                dr["ORDER_SE_SE_SE"] = item["batch_seq"].ToInt32();	//采购订单子子单身
                dr["ITEM_CODE"] = item["item_no"];	//品号
                dr["ITEM_NAME"] = item["item_name"];	//品名
                dr["ITEM_DESCRIPTION"] = item["item_spec"];	//规格
                dr["WAREHOUSE_CODE"] = item["warehouse_no"];	//仓库
                dr["UNIT_CODE"] = item["unit_no"];	//单位
                dr["PU_QTY"] = item["purchase_qty"].ToDecimal();	//采购量
                dr["UNARR_QTY"] = item["unpaid_qty"].ToDecimal();	//未交量
                dr["ACTUAL_QTY"] = item["qty"].ToDecimal();	//实发量
                dr["RECEIPT_QTY"] = item["receipt_qty"].ToDecimal();	//收货量
                dr["TAX_INVOICE_NO"] = string.Empty;	//发票号
                dr["PACKING_QTY"] = item["box_qty"].ToDecimal();	//箱装量
                dr["BC_TYPE"] = item["barcode_type"];	//条码类型

                dr["CHECK_NO"] = item["qc_type"];
                dr["PRICE_UNIT_CODE"] = item["valuation_unit_no"];
                dr["PRICE_QTY"] = item["valuation_qty"];
                dr["RECEIPT_OVER_RATE"] = item["over_deliver_rate"];
                dr["ITEM_FEATURE_CODE"] = item["item_feature_no"];
                dr["STATUS"] = item["receipt_status"];
                //系统管理字段
                dr["CreateBy"] = loginSrv.CurrentUserId;
                dr["CreateDate"] = DateTime.Now;
                dr["ModifiedBy"] = loginSrv.CurrentUserId;
                dr["ModifiedDate"] = DateTime.Now;
                dr["LastModifiedBy"] = loginSrv.CurrentUserId;
                dr["LastModifiedDate"] = DateTime.Now;

                //添加至dataTable中
                dataTable.Rows.Add(dr);
            }
            return dataTable;
        }

        /// <summary>
        /// 创建结构-CreateFilArrivalInfo[送货单]
        /// </summary>
        /// <returns></returns>
        private DataTable CreateFilArrivalInfo() {
            DataTable dataTable = new DataTable("FIL_ARRIVAL");
            //定义列
            dataTable.Columns.Add("FIL_ARRIVAL_ID", typeof(Guid));
            dataTable.Columns.Add("BOOK_CODE", typeof(string));
            dataTable.Columns.Add("PLANT_CODE", typeof(string));
            dataTable.Columns.Add("DOC_NO", typeof(string));
            dataTable.Columns.Add("DOC_DATE", typeof(DateTime));
            dataTable.Columns.Add("COMPANY_CODE", typeof(string));
            dataTable.Columns.Add("CATEGORY", typeof(string));
            dataTable.Columns.Add("ADDR_NAME", typeof(string));
            dataTable.Columns.Add("ARRIVAL_NO", typeof(string));
            dataTable.Columns.Add("PRINT", typeof(Int32));
            dataTable.Columns.Add("REMARK", typeof(string));
            dataTable.Columns.Add("STATUS", typeof(string));
            //系统管理字段
            dataTable.Columns.Add("CreateBy", typeof(object));
            dataTable.Columns.Add("CreateDate", typeof(DateTime));
            dataTable.Columns.Add("ModifiedBy", typeof(object));
            dataTable.Columns.Add("ModifiedDate", typeof(DateTime));
            dataTable.Columns.Add("LastModifiedBy", typeof(object));
            dataTable.Columns.Add("LastModifiedDate", typeof(DateTime));
            return dataTable;
        }

        /// <summary>
        /// 创建结构-CreateFilArrivalDInfo[送货单单身]
        /// </summary>
        /// <returns></returns>
        private DataTable CreateFilArrivalDInfo() {
            DataTable dataTable = new DataTable("FIL_ARRIVAL.FIL_ARRIVAL_D");
            //定义列
            dataTable.Columns.Add("FIL_ARRIVAL_D_ID", typeof(Guid));
            dataTable.Columns.Add("ParentId", typeof(Guid));
            dataTable.Columns.Add("BOOK_CODE", typeof(string));
            dataTable.Columns.Add("PLANT_CODE", typeof(string));
            dataTable.Columns.Add("DOC_NO", typeof(string));
            dataTable.Columns.Add("SequenceNumber", typeof(string));
            dataTable.Columns.Add("COMPANY_CODE", typeof(string));
            dataTable.Columns.Add("ORDER_NO", typeof(string));
            dataTable.Columns.Add("ORDER_SE", typeof(Int32));
            dataTable.Columns.Add("ORDER_SE_SE", typeof(Int32));
            dataTable.Columns.Add("ORDER_SE_SE_SE", typeof(Int32));
            dataTable.Columns.Add("ITEM_CODE", typeof(string));
            dataTable.Columns.Add("ITEM_NAME", typeof(string));
            dataTable.Columns.Add("ITEM_DESCRIPTION", typeof(string));
            dataTable.Columns.Add("WAREHOUSE_CODE", typeof(string));
            dataTable.Columns.Add("UNIT_CODE", typeof(string));
            dataTable.Columns.Add("PU_QTY", typeof(decimal));
            dataTable.Columns.Add("UNARR_QTY", typeof(decimal));
            dataTable.Columns.Add("ACTUAL_QTY", typeof(decimal));
            dataTable.Columns.Add("RECEIPT_QTY", typeof(decimal));
            dataTable.Columns.Add("TAX_INVOICE_NO", typeof(string));
            dataTable.Columns.Add("PACKING_QTY", typeof(decimal));
            dataTable.Columns.Add("BC_TYPE", typeof(string));

            dataTable.Columns.Add("CHECK_NO", typeof(string));
            dataTable.Columns.Add("PRICE_UNIT_CODE", typeof(string));
            dataTable.Columns.Add("PRICE_QTY", typeof(decimal));
            dataTable.Columns.Add("RECEIPT_OVER_RATE", typeof(decimal));
            dataTable.Columns.Add("ITEM_FEATURE_CODE", typeof(string));
            dataTable.Columns.Add("STATUS", typeof(string));
            //系统管理字段
            dataTable.Columns.Add("CreateBy", typeof(object));
            dataTable.Columns.Add("CreateDate", typeof(DateTime));
            dataTable.Columns.Add("ModifiedBy", typeof(object));
            dataTable.Columns.Add("ModifiedDate", typeof(DateTime));
            dataTable.Columns.Add("LastModifiedBy", typeof(object));
            dataTable.Columns.Add("LastModifiedDate", typeof(DateTime));
            return dataTable;
        }

        /// <summary>
        ///     通过DataTable列名转换为ColumnMappings
        /// </summary>
        /// <param name="columns">表的列的集合</param>
        /// <returns>Mapping集合</returns>
        private List<BulkCopyColumnMapping> GetBulkCopyColumnMapping(DataColumnCollection columns) {
            List<BulkCopyColumnMapping> mapping = new List<BulkCopyColumnMapping>();
            foreach (DataColumn column in columns) {
                var targetName = column.ColumnName;//列名
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if ((targetName.IndexOf("_", StringComparison.Ordinal) > 0)
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //列名长度
                    var nameLength = targetName.Length;
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_", StringComparison.Ordinal) + 1;
                    //拼接目标字段名
                    targetName = targetName.Substring(0, endPos - 1) + "." +
                                 targetName.Substring(endPos, nameLength - endPos);
                }
                BulkCopyColumnMapping mappingItem = new BulkCopyColumnMapping(column.ColumnName, targetName);
                mapping.Add(mappingItem);
            }
            return mapping;
        }
    }
}
