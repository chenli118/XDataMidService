using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace XDataMidService.BPImp
{
    /// <summary>
    /// 解压新纪元001文件
    /// 分x86,x64
    /// </summary>
    public class UnZipByCom
    {
        private readonly static object unObject = new object();

        internal class x86TU2Bck
        {
            /// <summary>
            /// x86
            /// </summary>
            /// <param name="filepath"></param>
            /// <param name="unbckpath"></param>
            /// <returns></returns>
            [DllImport("x86TU2Bck.dll", CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode)]
            public extern static int UnBckFile(string filepath, string unbckpath);

            // EntryPoint = "UnBckStream",CharSet = CharSet.Unicode)]

            [DllImport("x86TU2Bck.dll", CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode)]
            public extern static int UnBckStream(IntPtr Buffer, int iSize, string unbckpath);
        }

        internal class x64TU2Bck
        {
            /// <summary>
            /// x64
            /// </summary>
            /// <param name="filepath"></param>
            /// <param name="unbckpath"></param>
            /// <returns></returns>
            [DllImport("x64TU2Bck.dll", CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode)]
            public extern static int UnBckFile(string filepath, string unbckpath);

            [DllImport("x64TU2Bck.dll", CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode)]
            public extern static int UnBckStream(IntPtr Buffer, int iSize, string unbckpath);
        }

        /// <summary>
        /// 解压001文件
        /// </summary>
        /// <param name="un001Filepath">001文件带路径</param>
        /// <param name="unPath">解压到目标目录</param>
        /// <returns>true 成功,false失败</returns>
        internal static bool UnZipFile(string un001Filepath, string unPath)
        {   
            var fileinfo = new System.IO.FileInfo(un001Filepath);
            int iRet = -1;
            fileinfo.MoveTo(un001Filepath.Replace(".001", ".bck"));
            if (IntPtr.Size == 8)
            {
                 iRet = x64TU2Bck.UnBckFile(fileinfo.FullName, unPath);
            }
            else
            {
                iRet = x86TU2Bck.UnBckFile(fileinfo.FullName, unPath);
              
            }
            return iRet == 0 ? true : false;
        }

        /// <summary>
        /// 解压001文件
        /// </summary>
        /// <param name="un001Stream">001文件流</param>
        /// <param name="unPath">解压到目标目录</param>
        /// <returns>true 成功,false失败</returns>
        internal static bool UnZipFile(System.IO.Stream un001Stream, string unPath)
        {
            lock (unObject)
            {
                int len = (int)un001Stream.Length;
                byte[] bytes = new byte[len];
                un001Stream.Read(bytes, 0, len);

                IntPtr Buffer = Marshal.AllocHGlobal(len);
                Marshal.Copy(bytes, 0, Buffer, len);
                try
                {
                    int iRet = x86TU2Bck.UnBckStream(Buffer, len, unPath + "\\");
                    return iRet == 0 ? true : false;
                }
                catch (Exception ex)
                {
                   
                    int iRet = x64TU2Bck.UnBckStream(Buffer, len, unPath + "\\");
                    return iRet == 0 ? true : false;
                    throw new Exception("执行tu2bck失败!" + ex);
                }
                finally
                {
                    Marshal.FreeHGlobal(Buffer);
                }
            }
        }
    }
}
