# SCID to NinjaTrader 8 Converter

A high-performance Python tool for converting Sierra Chart SCID tick data files to NinjaTrader 8 import format with automatic outlier detection and cleaning.

## 🚀 Features

- **Fast Parallel Processing**: Multi-core CPU utilization for rapid conversion
- **Automatic Cleaning**: Intelligent outlier detection using rolling median filters
- **Symbol-Specific Thresholds**: Pre-configured cleaning parameters for popular symbols
- **Batch Processing**: Convert multiple files at once with wildcard support
- **Tick Replay Format**: Full bid/ask data for volumetric analysis
- **Sub-second Precision**: 7 decimal places for accurate timestamp representation

## 📋 Supported Symbols & Thresholds

| Symbol | Cleaning Threshold (points) | Description |
|--------|-----------------------------|-------------|
| MGC    | 25.0                       | Micro Gold |
| MNQ    | 25.0                       | Micro Nasdaq |
| GC     | 25.0                       | Gold |
| NQ     | 25.0                       | Nasdaq |
| ES     | 25.0                       | S&P 500 |
| MES    | 25.0                       | Micro S&P 500 |
| CL     | 50.0                       | Crude Oil |
| MCL    | 50.0                       | Micro Crude Oil |
| YM     | 50.0                       | Dow Jones |
| MYM    | 50.0                       | Micro Dow Jones |

## 🛠️ Installation

### Prerequisites
- Python 3.7 or higher
- Multi-core CPU (recommended for optimal performance)

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/scid-converter.git
cd scid-converter

# Install dependencies
pip install -r requirements.txt

# Or install as a package
pip install .
```

## � Getting SCID Files from Sierra Chart

Before you can convert files, you need to download tick data from Sierra Chart.

### Prerequisites
- **Purchase Sierra Chart Service Package 10** (~$36/month) — Includes free historical tick data access
- **Download and Install Sierra Chart** from [sierrachart.com](https://www.sierrachart.com)

### Step-by-Step Guide

#### 1. Connect to Data Feed
- Open Sierra Chart
- Go to **File → Connect to Data Feed**
- Wait for connection confirmation

#### 2. Configure Tick Storage
- Go to **File → Data/Trade Service Settings**
- Set **Intraday Data Storage Time Unit** = `1 Tick`
- Set **Allow Support for Sierra Chart Data Feeds** = `Yes`
- Under **Main Settings**, set **Current Selected Service** = `SC Data - All Services`
- Click **Save** and **Apply All**

#### 3. Open a Symbol Chart
- Go to **File → Find Symbol**
- Search for your desired symbol (e.g., `ES`, `NQ`, `CL`, `MGC`, `MNQ`)
- Select **Open Intraday Chart**

#### 4. Configure Chart for Tick Data
Right-click on chart → **Chart Settings** (or press `F5`)

**Bar Period Tab:**
- Set **Bar Period Type** = `Number of Trades Per Bar`
- Set **Bar Period** = `1` (each bar = 1 trade)

**Data Limiting Tab:**
- Set **Days to Load for Intraday Chart Type** = `4380` (≈12 years max)

**Symbol Tab (for Futures):**
- Set **Continuous Contract** = `Continuous Futures Contract - Volume Based Rollover, Back Adjusted`

Click **Apply All**

#### 5. Wait for Download
- Sierra Chart will download all available tick data
- This may take several minutes depending on the symbol and date range
- Progress is shown in the status bar

#### 6. Locate Your Data Files
- Data files are saved to: `C:\SierraChart\Data\`
- Files have `.scid` extension (e.g., `ESH25.scid`, `MGCZ25-COMEX.scid`)
- Use this converter to transform them to NinjaTrader 8 format!

**💡 Tip:** For continuous contracts, Sierra Chart creates separate files for each contract month. You can batch convert all files at once using this tool.

## �📖 Usage

### Single File Conversion
```bash
python scid_to_nt8_converter.py MNQZ25-CME.scid
```

### Batch Processing
```bash
# Convert all MNQ files in current directory
python scid_to_nt8_converter.py --batch

# Convert all MNQ files recursively
python scid_to_nt8_converter.py --batch-all

# Convert all MGC files recursively
python scid_to_nt8_converter.py --batch-mgc-all
```

### Raw Conversion (No Cleaning)
```bash
python scid_to_nt8_converter.py --raw MNQZ25-CME.scid
```

### Custom Threshold
```bash
python scid_to_nt8_converter.py --threshold 30.0 MNQZ25-CME.scid
```

## 📊 Output Format

The converter produces NinjaTrader 8 compatible tick data in the following format:

```
yyyyMMdd HHmmss fffffff;last;bid;ask;volume
```

**Example:**
```
20250128 1430001234567;4521.25;4521.00;4521.50;100
20250128 1430002345678;4521.30;4521.25;4521.75;150
```

## 🔧 NinjaTrader 8 Import Settings

1. Open NinjaTrader 8
2. Go to `Tools > Import > Historical Data`
3. Select the converted file
4. Configure import settings:
   - **Format**: "NinjaTrader (start of bar timestamps)"
   - **Data Type**: "Last"
   - **Time Zone**: "UTC"
5. Click "Import"

## 🧹 Cleaning Algorithm

The converter uses a sophisticated cleaning pipeline:

1. **Rolling Median Filter**: Calculates median price over a configurable window (default: 200 ticks)
2. **Threshold Detection**: Identifies ticks deviating beyond symbol-specific thresholds
3. **Outlier Removal**: Removes spikes and illegal ticks while preserving valid data
4. **Validation**: Ensures bid ≤ last ≤ ask price relationships

## ⚡ Performance

- **Parallel Processing**: Utilizes all available CPU cores
- **Memory Efficient**: Processes files in chunks to handle large datasets
- **Optimized I/O**: Buffered reading/writing for maximum throughput

**Typical Performance:**
- 1M ticks: ~10 seconds (8-core CPU)
- 10M ticks: ~90 seconds (8-core CPU)

## 📁 Project Structure

```
scid-converter/
├── scid_to_nt8_converter.py    # Main converter script
├── requirements.txt            # Python dependencies
├── setup.py                    # Package setup
├── README.md                   # This file
├── LICENSE                     # MIT License
├── .gitignore                  # Git ignore file
├── examples/                   # Usage examples
│   ├── batch_conversion.py
│   └── custom_threshold.py
└── tests/                      # Test suite
    ├── test_converter.py
    └── test_cleaning.py
```

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup
```bash
# Clone repository
git clone https://github.com/yourusername/scid-converter.git
cd scid-converter

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .

# Run tests
python -m pytest tests/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Sierra Chart for the SCID file format specification
- NinjaTrader for the import format requirements
- The Python community for excellent multiprocessing libraries

## 📞 Support

- 📧 Email: your-email@example.com
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/scid-converter/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/yourusername/scid-converter/discussions)

## 🔗 Related Projects

- [SCID to MT5 Converter](https://github.com/yourusername/scid-mt5-converter) - Convert to MetaTrader 5 format
- [SCID Web Converter](https://github.com/yourusername/scid-web-converter) - Browser-based conversion tool

---

**⭐ Star this repository if you find it useful!**
