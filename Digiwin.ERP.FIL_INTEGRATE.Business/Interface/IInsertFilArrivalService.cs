//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>建立送货单接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    [TypeKeyOnly]
    [Description("建立送货单接口")]
    public interface IInsertFilArrivalService {
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
        /// <param name="deliveryDetail">单身数据集</param>
        void InsertFilArrival(string site_no, string delivery_no, string create_date, string supplier_no, string purchase_type, string receipt_address,
            string receipt_no, string print_qty, string remark, string status, DependencyObjectCollection delivery_detail);
    }
}
