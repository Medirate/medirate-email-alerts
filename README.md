# 🚀 Modern Streamlit Dashboard

A beautiful, interactive dashboard built with Streamlit featuring modern design, real-time analytics, and responsive UI components.

## ✨ Features

- **🎨 Modern Design**: Gradient backgrounds, hover effects, and smooth animations
- **📊 Interactive Charts**: Real-time data visualization with Plotly
- **📱 Responsive Layout**: Works perfectly on desktop, tablet, and mobile
- **🎛️ Multi-page Navigation**: Home, Dashboard, Analytics, and Settings pages
- **⚡ Real-time Updates**: Dynamic data and interactive components
- **🎯 Custom Styling**: Beautiful CSS animations and modern UI elements

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- Azure Blob Storage account
- PostgreSQL database

### Installation

1. **Clone or download this project**
   ```bash
   cd streamlit-project
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp env_example.txt .env
   # Edit .env file with your actual credentials
   ```

5. **Run the application**
   ```bash
   streamlit run app.py
   ```

6. **Open your browser**
   Navigate to `http://localhost:8501`

## 📁 Project Structure

```
streamlit-project/
├── app.py              # Main Streamlit application
├── requirements.txt    # Python dependencies
└── README.md          # Project documentation
```

## 🎨 Pages Overview

### 🏠 Home Page
- Welcome header with gradient text
- Key metrics cards with hover effects
- Interactive line chart
- Feature highlights
- Call-to-action button

### 📊 Dashboard Page
- Interactive data visualization
- Customizable chart types (Line, Bar, Area)
- Date range and metric filters
- Real-time data updates

### 📈 Analytics Page
- Pie chart for category analysis
- 3D scatter plot visualization
- Advanced data insights

### ⚙️ Settings Page
- Theme customization
- Notification preferences
- Privacy settings
- User preferences

### 🔄 Data Processing Page
- **Bill Track Processing**: Process Medicaid Rates bill sheet data
- **Provider Alerts Processing**: Process provider alerts data
- **Connection Status Check**: Verify Azure and database connections
- **Real-time Logging**: See detailed progress and status updates
- **Combined Processing**: Run both processes sequentially

## 🛠️ Customization

### Adding New Pages
1. Add a new option to the sidebar selectbox
2. Create a new `elif` condition in the main content area
3. Add your page content and functionality

### Styling
The app uses custom CSS for modern styling. You can modify the styles in the `st.markdown()` section at the top of `app.py`.

### Data Sources
Currently uses sample data generated with NumPy. Replace with your actual data sources:
- CSV files
- Database connections
- API endpoints
- Real-time data streams

## 📦 Dependencies

- **streamlit**: Web application framework
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **azure-storage-blob**: Azure Blob Storage integration
- **python-dotenv**: Environment variable management
- **psycopg2-binary**: PostgreSQL database connector
- **openpyxl**: Excel file processing
- **pillow**: Image processing (for future enhancements)

## 🎯 Future Enhancements

- [ ] Database integration
- [ ] User authentication
- [ ] Real-time data streaming
- [ ] Export functionality
- [ ] Dark mode toggle
- [ ] More chart types
- [ ] Mobile app version

## 🤝 Contributing

1. Fork the project
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 🆘 Support

If you encounter any issues or have questions:
1. Check the [Streamlit documentation](https://docs.streamlit.io/)
2. Review the error messages in your terminal
3. Ensure all dependencies are installed correctly

---

**Built with ❤️ using Streamlit** 