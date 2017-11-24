//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>供应商检查服务</description>
//---------------------------------------------------------------- 
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [ServiceClass(typeof(ISupplierService))]
    [Description("供应商检查服务")]
    public class SupplierService : ServiceComponent, ISupplierService {

        /// <summary>
        /// 根据传入的供应商编号检查供应商是否存在，若存在返回供应商编号和名称
        /// </summary>
        /// <param name="supplier_no">供应商编号</param>
        /// <returns></returns>
        public Hashtable Supplier(string supplier_no) {
            try {
                IInfoEncodeContainer infoCode = GetService<IInfoEncodeContainer>();//信息编码服务
                if (Maths.IsEmpty(supplier_no)) {
                    throw new BusinessRuleException(infoCode.GetMessage("A111201", "supplier_no"));//‘入参【supplier_no】未传值’
                }
                //获取供应商信息
                QueryNode node = GetSupplier(supplier_no);
                DependencyObjectCollection supplierData = GetService<IQueryService>().ExecuteDependencyObject(node);
                //Query.Count=0
                if (supplierData.Count==0) {
                    throw new BusinessRuleException(infoCode.GetMessage("A100711"));//报错：供应商不存在
                }

                //组织返回结果
                Hashtable result = new Hashtable{
                    {"supplier_no", supplierData[0]["SUPPLIER_CODE"]},
                    {"supplier_name", supplierData[0]["SUPPLIER_NAME"]}
                };
                //供应商编号
                //供应商名称
                return result;
            } catch (Exception) {
                throw;
            }
        }

        /// <summary>
        /// 查询供应商信息
        /// </summary>
        /// <param name="supplierNo">供应商编号</param>
        /// <returns>供应商编号\供应商名称</returns>
        private QueryNode GetSupplier(string supplierNo) {
            return OOQL.Select(
                                    OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "SUPPLIER_CODE"),
                                    OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "SUPPLIER_NAME"))
                                .From("SUPPLIER", "SUPPLIER")
                                .Where((OOQL.AuthFilter("SUPPLIER", "SUPPLIER"))
                                        &(OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(supplierNo)));
        }
    }
}
