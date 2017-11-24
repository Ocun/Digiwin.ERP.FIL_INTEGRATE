//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-13</createDate>
//<description>供应商平台信息获取服务</description>
//---------------------------------------------------------------- 
//20170428 modi by liwei1 for P001-161209002
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement{
    /// <summary>
    /// 供应商平台信息获取服务
    /// </summary>
    [ServiceClass(typeof (IGetSupplierInfoService))]
    [Description("供应商平台信息获取服务")]
    public class GetSupplierInfoService : ServiceComponent, IGetSupplierInfoService{
        /// <summary>
        /// 根据传入的供应商编号获取供应商其他相关信息
        /// </summary>
        /// <param name="supplier_no">供应商编号</param>
        /// <returns></returns>
        public Hashtable GetSupplierInfo(string supplier_no){
            try{
                #region //20170428 modi by liwei1 for P001-161209002
                //if (Maths.IsEmpty(supplier_no)) {
                //    IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                //    throw new BusinessRuleException(infoEnCode.GetMessage("A111201", "supplier_no"));//‘入参【supplier_no】未传值’( A111201)
                //}
                #endregion

                //根据传入的供应商编号获取供应商其他相关信息
                QueryNode queryNode = GetSupplierDetail(supplier_no);
                DependencyObjectCollection supplierDetail =
                    this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组合返回结果
                Hashtable result = new Hashtable{{"supplier_detail", supplierDetail}};
                return result;
            }
            catch (Exception){
                throw;
            }
        }

        /// <summary>
        /// 根据传入的供应商编号获取供应商其他相关信息
        /// </summary>
        /// <param name="supplierNo">供应商编号</param>
        /// <returns></returns>
        private QueryNode GetSupplierDetail(string supplierNo){
            #region //20170428 add by liwei1 for P001-161209002
            if (Maths.IsEmpty(supplierNo)){
                return OOQL.Select(
                    OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "supplier_no"),
                    OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "supplier_name"))
                    .From("SUPPLIER", "SUPPLIER")
                    .Where(OOQL.AuthFilter("SUPPLIER", "SUPPLIER"));
            }
            #endregion

            return OOQL.Select(
                //OOQL.CreateConstants(supplier_no, "supplier_no"),//20170428 mark by liwei1 for P001-161209002
                OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "supplier_no"),
                //20170428 add by liwei1 for P001-161209002
                OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "supplier_name"))
                .From("SUPPLIER", "SUPPLIER")
                .Where((OOQL.AuthFilter("SUPPLIER", "SUPPLIER"))
                       & (OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(supplierNo)));

        }
    }
}
