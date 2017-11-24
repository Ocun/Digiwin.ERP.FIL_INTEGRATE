//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-09</createDate>
//<description>异步返回值对象</description>
//----------------------------------------------------------------
using System;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [Description("异步返回值对象")]
    public class AsyncResultObject {
        /// <summary>
        /// 标识异步处理已完成，刷新机制将会读取此标识，
        /// </summary>
        public bool IsCompleted { get; set; }

        /// <summary>
        /// 异常信息
        /// </summary>
        public Exception Exception { get; set; }
        
        /// <summary>
        /// 服务处理逻辑是否发生异常
        /// </summary>
        public bool IsHappenException { get; set; }

        /// <summary>
        /// 服务返回值
        /// </summary>
        public DependencyObjectCollection Result { get; set; }
    }
}
