using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Challenge1_Yusuf_Akbas
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting...");
            SalesDataHelper salesData = new SalesDataHelper();
            Console.WriteLine("Getting data...");
            System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var items = salesData.GetStreamingSales(SAMPLE_SIZE);

            var summaryItems = Challenge1Solution(items);

            SaveToDatabase(summaryItems);

            stopwatch.Stop();
            Console.WriteLine($"Processing sales data completed. Total time: {stopwatch.Elapsed.TotalSeconds} seconds.");
            Console.ReadKey();
        }

        private const int SAMPLE_SIZE = 10000000;
        private static IEnumerable<SummarizedSalesData> Challenge1Solution(IEnumerable<SalesData> items)
        {
            return new SummarizeSalesData().GetSummarizedSalesDatasAsync(items).Result;
        }

        private static void SaveToDatabase(IEnumerable<SummarizedSalesData> items)
        {
            // Assumed database fast insert code is implemented here.
            Console.WriteLine("Writing records to the database...");
            int recordCounter = 0;
            double totalVolume = 0;
            decimal totalPrice = 0;
            foreach (var item in items)
            {
                recordCounter++;
                totalVolume += item.TotalVolume;
                totalPrice += item.TotalPrice;
            }
            Console.WriteLine($"Records saved to the database. Total record count: {recordCounter}, Total Volume: {totalVolume}, Total Price: {totalPrice}");
        }
    }

    /// <summary>
    /// Summarizes sales data.
    /// </summary>
    public sealed class SummarizeSalesData : IDisposable
    {

        #region Properties

        /// <summary>
        /// Thread-safe list holding <see cref="DtoSalesData"/> read from the stream of type <see cref="SalesData"/>.
        /// The data added by the Compute method is consumed by the Run method.
        /// </summary>
        private System.Collections.Concurrent.BlockingCollection<DtoSalesData> ReadedSales { get; set; }

        /// <summary>
        /// Thread-safely stores data going to subprocess.
        /// </summary>
        private Dictionary<int, System.Collections.Concurrent.BlockingCollection<DtoSalesData>> ProcessSalesDataQuery { get; set; }

        /// <summary>
        /// Stores the subprocess lists.
        /// After the processes are finished, the result output is taken with this property.
        /// </summary>
        private List<SubProcessForBrandId> Results { get; set; }

        /// <summary>
        /// If true current instance been disposed.
        /// </summary>
        private bool Disposed { get; set; } = false;

        #endregion

        #region Public Methods

        /// <summary>
        /// Summarizes sales data.
        /// </summary>
        /// <param name="salesDataStream"><see cref="SalesData" data stream/></param>
        /// <param name="autoDispose">If true, dispose current instance after the data has been read</param>
        /// <returns>List of summarized sales data</returns>
        public async System.Threading.Tasks.Task<IEnumerable<SummarizedSalesData>> GetSummarizedSalesDatasAsync(IEnumerable<SalesData> salesDataStream, bool autoDispose = false)
        {

            ReadedSales = new();
            Results = new();
            var main = System.Threading.Tasks.Task.Factory.StartNew(Run, System.Threading.Tasks.TaskCreationOptions.LongRunning);

            foreach (SalesData item in salesDataStream)
            {

                ReadedSales.Add(new DtoSalesData
                {
                    BrandId = item.BrandId.AsMemory(),
                    CompanyId = item.CompanyId,
                    Price = item.Price,
                    ProductId = item.ProductId,
                    SalesDate = item.SalesDate.AsMemory(),
                    StoreId = item.StoreId.AsMemory(),
                    Volume = item.Volume
                });

            }

            ReadedSales.CompleteAdding();
            await main;
            return GetDatas(autoDispose);

        }

        /// <summary>
        /// Releases all resources used by the current instance of the <see cref="SummarizeSalesData"/> class.
        /// </summary>
        public void Dispose()
        {

            if (!Disposed)
            {

                ReadedSales.Dispose();

                foreach (SubProcessForBrandId subProcess in Results)
                {

                    subProcess.Dispose();

                }

                ProcessSalesDataQuery.Clear();
                Results.Clear();
                ProcessSalesDataQuery = null;
                Results = null;
                ReadedSales = null;
                Disposed = true;

            }

            GC.SuppressFinalize(this);

        }

        #endregion

        #region Private Methods

        /// <summary>
        /// It consumes the read sales data and divides the read data into subprocess by CompanyId.
        /// </summary>
        private void Run()
        {

            ProcessSalesDataQuery = new();

            foreach (DtoSalesData salesData in ReadedSales.GetConsumingEnumerable())
            {

                if (ProcessSalesDataQuery.TryGetValue(salesData.CompanyId, out System.Collections.Concurrent.BlockingCollection<DtoSalesData> collection))
                {

                    collection.Add(salesData);

                }
                else
                {

                    System.Collections.Concurrent.BlockingCollection<DtoSalesData> col = new() { salesData };
                    ProcessSalesDataQuery.Add(salesData.CompanyId, col);
                    SubProcessForBrandId process = new(col);
                    Results.Add(process);
                    System.Threading.Tasks.Task.Factory.StartNew(process.Run, System.Threading.Tasks.TaskCreationOptions.AttachedToParent);

                }

            }

            foreach (var item in ProcessSalesDataQuery.Values)
                item.CompleteAdding();

        }

        /// <summary>
        /// It combines the data summarized in subprocesses and returns a single list.
        /// </summary>
        /// <param name="autoDispose">If true, dispose current instance after the data has been read</param>
        /// <returns>List of summarized sales data</returns>
        private IEnumerable<SummarizedSalesData> GetDatas(bool autoDispose)
        {

            foreach (SubProcessForBrandId subProcess in Results)
            {

                foreach (SummarizedSalesData data in subProcess.Result)
                {

                    yield return data;

                }

            }

            if (autoDispose)
                Dispose();

        }

        #endregion

        #region Classes

        /// <summary>
        /// Summarizes the sales data by the same CompanyId
        /// </summary>
        private sealed class SubProcessForBrandId : IDisposable
        {

            #region Properties

            /// <summary>
            /// List of summarized sales data
            /// </summary>
            public IEnumerable<SummarizedSalesData> Result { get => Results.Values; }

            /// <summary>
            /// List of summarized sales data
            /// </summary>
            private Dictionary<string, SummarizedSalesData> Results { get; init; }

            /// <summary>
            /// List of sales data to be processed.
            /// </summary>
            private System.Collections.Concurrent.BlockingCollection<DtoSalesData> SalesQuery { get; init; }

            /// <summary>
            /// If true current instance been disposed.
            /// </summary>
            private bool Disposed { get; set; } = false;

            #endregion

            public SubProcessForBrandId(System.Collections.Concurrent.BlockingCollection<DtoSalesData> salesQuery)
            {
                SalesQuery = salesQuery;
                Results = new();
            }

            #region Public Methods

            /// <summary>
            /// Satış verilerini özetler.
            /// </summary>
            public void Run()
            {

                foreach (DtoSalesData salesData in SalesQuery.GetConsumingEnumerable())
                {

                    int yearWeek = salesData.WeekOfYear;
                    string key = $"{salesData.BrandId}{salesData.ProductId}{salesData.StoreId}{yearWeek}";

                    if (Results.TryGetValue(key, out SummarizedSalesData summarizedSalesData))
                    {

                        summarizedSalesData.TotalPrice += salesData.Price;
                        summarizedSalesData.TotalVolume += salesData.Volume;

                    }
                    else
                    {

                        Results.Add(key, new SummarizedSalesData
                        {
                            StoreId = salesData.StoreId.ToString(),
                            BrandId = salesData.BrandId.ToString(),
                            ProductId = salesData.ProductId,
                            CompanyId = salesData.CompanyId,
                            TotalPrice = salesData.Price,
                            TotalVolume = salesData.Volume,
                            WeekNumber = yearWeek
                        });

                    }

                }

            }

            /// <summary>
            /// Releases all resources used by the current instance of the <see cref="SubProcessForBrandId"/> class.
            /// </summary>
            public void Dispose()
            {

                if (!Disposed)
                {

                    SalesQuery.Dispose();
                    Results.Clear();
                    Disposed = true;

                }

                GC.SuppressFinalize(this);

            }

            #endregion

        }

        /// <summary>
        /// Data transfer object of type <see cref="SalesData"/>
        /// </summary>
        private sealed class DtoSalesData
        {

            public int ProductId { get; set; }

            public ReadOnlyMemory<char> StoreId { get; set; }

            public ReadOnlyMemory<char> BrandId { get; set; }

            public int CompanyId { get; set; }

            public ReadOnlyMemory<char> SalesDate { get; set; }

            public double Volume { get; set; }

            public decimal Price { get; set; }

            /// <summary>
            /// 
            ///  Returns the week of the year that includes the date in the specified
            ///  <see cref="SalesDate"/> value.
            /// 
            /// </summary>
            public int WeekOfYear
            {
                get
                {
                    DateTime time = DateTime.Parse(SalesDate.ToString());
                    DateTime now = DateTime.Now;
                    int week = CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(time, CalendarWeekRule.FirstFullWeek, DayOfWeek.Monday);
                    int year = now.Month == 1 && week > 4 ? time.Year - 1 : time.Year;
                    int yearWeek = (year * 100) + week;
                    return yearWeek;
                }
            }


        }

        #endregion

    }

    public sealed class SalesDataHelper
    {
        public SalesDataHelper()
        {
            _rnd = new Random();
            InitArray(ref _brands, ref _brandCodes, 10, "Brand Text", true);
            InitArray(ref _companies, ref _companyCodes, 5, "Company Name", true);
            InitArray(ref _stores, ref _storeCodes, 100, "Store Name", true);
            InitArray(ref _prodcuts, ref _prodcuts, 50, "Product description", false);
        }

        private Random _rnd;
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public IEnumerable<SalesData> GetStreamingSales(long maxCount)
        {
            long counter = 0;
            while (counter++ < maxCount)
            {
                int brandIndex = _rnd.Next(0, _brands.Length);
                int companyIndex = _rnd.Next(0, _companies.Length);
                int storeIndex = _rnd.Next(0, _stores.Length);
                int productIndex = _rnd.Next(0, _prodcuts.Length);
                var item = new SalesData()
                {
                    Id = counter,
                    BrandId = _brandCodes[brandIndex],
                    BrandName = _brands[brandIndex],
                    CompanyId = companyIndex,
                    CompanyName = _companies[companyIndex],
                    ProductId = productIndex,
                    ProductName = _prodcuts[productIndex],
                    StoreId = _storeCodes[storeIndex],
                    StoreName = _stores[storeIndex],
                    Price = (decimal)(_rnd.NextDouble() * 1000),
                    Volume = Math.Round(_rnd.NextDouble() * 1000, 2),
                    SalesDate = DateTime.Today.AddDays(-1 * _rnd.Next(1, 60)).ToString("yyyy-MM-dd"),
                    OtherData = new byte[10 * 1024] // 10KB
                };
                _rnd.NextBytes(item.OtherData);
                yield return item;
            }
        }

        private void InitArray(ref string[] array, ref string[] idArray, int itemCount, string prefix, bool fillIdArray)
        {
            array = new string[itemCount];
            if (fillIdArray)
                idArray = new string[itemCount];
            for (int i = 0; i < itemCount; i++)
            {
                array[i] = $"{prefix} - {i.ToString()}";
                if (fillIdArray)
                {
                    string tmpCode;
                    do
                    {
                        tmpCode = new string(Enumerable.Repeat(chars, 5)
                          .Select(s => s[_rnd.Next(s.Length)]).ToArray());
                    } while (idArray.Contains(tmpCode));
                    idArray[i] = tmpCode;
                }
            }
        }


        private string[] _brands;
        private string[] _brandCodes;
        private string[] _companies;
        private string[] _companyCodes;
        private string[] _stores;
        private string[] _storeCodes;
        private string[] _prodcuts;
    }

    public sealed class SalesData
    {
        public long Id { get; set; }
        public int ProductId { get; set; }
        public string ProductName { get; set; }
        public string StoreId { get; set; }
        public string StoreName { get; set; }
        public string BrandId { get; set; }
        public string BrandName { get; set; }
        public int CompanyId { get; set; }
        public string CompanyName { get; set; }
        public string SalesDate { get; set; }
        public double Volume { get; set; }
        public decimal Price { get; set; }
        public byte[] OtherData { get; set; }
    }

    public sealed class SummarizedSalesData
    {
        public int ProductId { get; set; }
        public int CompanyId { get; set; }
        public string StoreId { get; set; }
        public string BrandId { get; set; }
        public int WeekNumber { get; set; }
        public double TotalVolume { get; set; }
        public decimal TotalPrice { get; set; }
    }
}
