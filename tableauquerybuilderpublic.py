import sys
import requests
import xml.etree.ElementTree as ET
import csv
from PyQt5.QtWidgets import QSplashScreen, QCheckBox, QDateEdit, QFrame, QSpinBox, QStackedWidget, QDialog, QApplication, QWidget, QVBoxLayout, QPushButton, QTextEdit, QLabel, QLineEdit, QComboBox, QAbstractItemView, QTableWidget, QTableWidgetItem, QFileDialog, QHBoxLayout, QListWidget, QGroupBox, QListWidgetItem, QTabWidget, QScrollArea, QSizePolicy, QFormLayout, QInputDialog, QMessageBox, QSpacerItem
from PyQt5.QtCore import Qt, QDate, pyqtSignal, QThread, QTimer
import threading
from PyQt5.QtGui import QDoubleValidator, QPixmap, QFont, QColor, QPainter
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import sqlite3
import os
import datetime
import json

# At the top of your file, after imports but before any class definitions
TableauAppClass = None

class QueryWorker(QThread):
    result_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    
    def __init__(self, auth_token, datasource_luid, fields, filters):
        super().__init__()
        self.auth_token = auth_token
        self.datasource_luid = datasource_luid
        self.fields = fields
        self.filters = filters
        self.is_cancelled = False
        
    def run(self):
        try:
            headers = {
                'X-Tableau-Auth': self.auth_token,
                'Content-Type': 'application/json'
            }
            url = f'https://{enter_your_cluster}.online.online.tableau.com/api/v1/vizql-data-service/query-datasource'
            
            payload = {
                "datasource": {
                    "datasourceLuid": self.datasource_luid
                },
                "query": {
                    "fields": self.fields,
                    "filters": self.filters
                }
            }
            
            # Check if cancelled before making the request
            if self.is_cancelled:
                return
                
            response = requests.post(url, headers=headers, json=payload)
            
            # Check if cancelled after the request
            if self.is_cancelled:
                return
                
            if response.status_code == 200:
                self.result_signal.emit(response.json())
            else:
                self.error_signal.emit(f'Error: {response.status_code}\n{response.text}')
        except Exception as e:
            if not self.is_cancelled:
                self.error_signal.emit(f"An error occurred: {e}")
    
    def cancel(self):
        self.is_cancelled = True

class DummyScheduler:
    """A dummy scheduler that just logs actions instead of executing them"""
    def add_job(self, func, *args, **kwargs):
        print(f"DummyScheduler: Would add job {kwargs.get('name', 'unnamed')} (real scheduler not available)")
        return None
        
    def get_job(self, job_id):
        return None
        
    def remove_job(self, job_id):
        print(f"DummyScheduler: Would remove job {job_id} (real scheduler not available)")

class TableauApp(QWidget):
    def __init__(self, headless=False):
        super().__init__()
        self.auth_token = None
        self.current_datasource_luid = None
        self.headless = headless

        # Initialize the re-authentication timer
        self.reauth_timer = QTimer(self)
        self.reauth_timer.timeout.connect(self.sign_in)
        self.reauth_timer.start(30 * 60 * 1000)  # Re-authenticate every 30 minutes

        # Set up the scheduler with error handling
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
            
            jobstores = {
                'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
            }
            self.scheduler = BackgroundScheduler(jobstores=jobstores)
            self.scheduler.start()
            print("Scheduler started successfully")
        except Exception as e:
            print(f"Error initializing scheduler: {e}")
            import traceback
            traceback.print_exc()
            # Create a fallback scheduler that just logs instead of executing
            self.scheduler = DummyScheduler()
        
        # Only initialize UI if not in headless mode
        if not headless:
            self.initUI()
            self.sign_in()
        
        # Load saved queries
        if not headless:
            self.load_queries_from_disk()
            self.load_schedules_from_disk()

    def initUI(self):
        self.setWindowTitle('Tableau Data Query Tool')
        self.setGeometry(100, 100, 1000, 700)  # Wider window to accommodate horizontal layout

        main_layout = QVBoxLayout()
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        
        # Create tabs
        self.query_tab = QWidget()
        self.results_tab = QWidget()
        self.filters_tab = QWidget()
        self.schedule_tab = QWidget()
        
        # Set up layouts for each tab
        query_layout = QVBoxLayout(self.query_tab)
        results_layout = QVBoxLayout(self.results_tab)
        filters_layout = QVBoxLayout(self.filters_tab)
        schedule_layout = QVBoxLayout(self.schedule_tab)
        
        # === QUERY TAB CONTENT ===
        
        # Data source selection (stays at the top, full width)
        datasource_group = QGroupBox("Data Source")
        datasource_layout = QVBoxLayout()

        # Search bar and list for data sources
        self.datasource_search = QLineEdit()
        self.datasource_search.setPlaceholderText("Search data sources...")
        self.datasource_search.textChanged.connect(self.filter_datasources)
        datasource_layout.addWidget(self.datasource_search)

        self.datasource_list = QListWidget()
        self.datasource_list.itemClicked.connect(self.on_datasource_selected)
        datasource_layout.addWidget(self.datasource_list)

        # Put Refresh, Fetch, and Reset buttons on the same line
        button_layout = QHBoxLayout()
        self.fetch_fields_button = QPushButton('Fetch Fields')
        self.fetch_fields_button.clicked.connect(self.fetch_fields)
        refresh_button = QPushButton("Refresh Data Sources")
        refresh_button.clicked.connect(self.populate_datasource_list)
        reset_button = QPushButton("Reset")
        reset_button.clicked.connect(self.reset_selections)
        button_layout.addWidget(self.fetch_fields_button)
        button_layout.addWidget(refresh_button)
        button_layout.addStretch(1)
        button_layout.addWidget(reset_button)

        # Add buttons to the horizontal layout with Fetch Fields on the left
        button_layout.addWidget(self.fetch_fields_button)
        button_layout.addWidget(refresh_button)

        # Add the button layout to the datasource layout
        datasource_layout.addLayout(button_layout)

        datasource_group.setLayout(datasource_layout)
        query_layout.addWidget(datasource_group)

        # Remove the separate Fetch Fields button since it's now in the button layout
        # query_layout.addWidget(self.fetch_fields_button)  # Remove this line

        # Create horizontal layout for Dimensions and Measures
        fields_layout = QHBoxLayout()
        
        # Dimensions section (left side)
        dimensions_group = QGroupBox("Dimensions")
        dimensions_layout = QVBoxLayout()
        
        self.dimensions_list = QListWidget()
        self.dimensions_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self.dimensions_list.itemSelectionChanged.connect(self.update_selected_dimensions_display)
        dimensions_layout.addWidget(QLabel('Select Dimensions (up to 10):'))
        dimensions_layout.addWidget(self.dimensions_list)
        
        self.selected_dimensions_display = QTextEdit()
        self.selected_dimensions_display.setReadOnly(True)
        dimensions_layout.addWidget(QLabel('Selected Dimensions:'))
        dimensions_layout.addWidget(self.selected_dimensions_display)
        
        dimensions_group.setLayout(dimensions_layout)
        fields_layout.addWidget(dimensions_group, 1)  # 1 is the stretch factor
        
        # Measures section (right side)
        measures_group = QGroupBox("Measures")
        self.measures_layout = QVBoxLayout()
        
        # Start with one measure row
        self.measure_rows = []
        self.add_measure_row()
        
        # Add button for more measures
        add_measure_button = QPushButton("Add Measure")
        add_measure_button.clicked.connect(self.add_measure_row)
        self.measures_layout.addWidget(add_measure_button)
        
        # Add stretch to push everything to the top
        self.measures_layout.addStretch(1)
        
        measures_group.setLayout(self.measures_layout)
        fields_layout.addWidget(measures_group, 1)  # 1 is the stretch factor
        
        # Add the horizontal layout to the main query layout
        query_layout.addLayout(fields_layout)
        
        # Add Run Query and Save button at the bottom
        # self.query_button = QPushButton('Run Query')
        # self.query_button.clicked.connect(self.query_data_source)
        # query_layout.addWidget(self.query_button)
        query_buttons_layout = QHBoxLayout()
        self.query_button = QPushButton('Run Query')
        self.query_button.clicked.connect(self.query_data_source)
        save_query_button = QPushButton('Save Query')
        save_query_button.clicked.connect(self.save_query)
        query_buttons_layout.addWidget(self.query_button)
        query_buttons_layout.addWidget(save_query_button)
        query_layout.addLayout(query_buttons_layout)
        
        # === FILTERS TAB CONTENT ===
        filters_layout.setSpacing(10)
        filters_layout.setContentsMargins(10, 10, 10, 10)

        # Create a scroll area for filters
        filters_scroll = QScrollArea()
        filters_scroll.setWidgetResizable(True)  # This is important
        filters_scroll.setMinimumHeight(400)     # Set a minimum height
        filters_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)  # Ensure vertical scrollbar appears when needed

        self.filters_container = QWidget()
        self.filters_container_layout = QVBoxLayout(self.filters_container)
        self.filters_container_layout.setSpacing(10)
        self.filters_container_layout.setContentsMargins(10, 10, 10, 10)
        self.filters_container_layout.addStretch(1)  # Keep this to push filters to the top
        filters_scroll.setWidget(self.filters_container)
        filters_layout.addWidget(filters_scroll)

        # Add filter button
        self.add_filter_button = QPushButton("Add Filter")
        self.add_filter_button.clicked.connect(self.show_add_filter_dialog)
        filters_layout.addWidget(self.add_filter_button)
        
        # Add Run Query and Save Query buttons to filters tab
        filters_buttons_layout = QHBoxLayout()
        run_query_button_filters = QPushButton('Run Query')
        run_query_button_filters.clicked.connect(self.query_data_source)
        save_query_button_filters = QPushButton('Save Query')
        save_query_button_filters.clicked.connect(self.save_query)
        filters_buttons_layout.addWidget(run_query_button_filters)
        filters_buttons_layout.addWidget(save_query_button_filters)
        filters_layout.addLayout(filters_buttons_layout)

        
        # Store active filters
        self.active_filters = []
        
        # === RESULTS TAB CONTENT ===
        
        # Results table
        self.result_table = QTableWidget()
        results_layout.addWidget(self.result_table)
        
        # Results text area
        self.result_area = QTextEdit()
        self.result_area.setReadOnly(True)
        results_layout.addWidget(self.result_area)
        
        # Export button
        self.export_button = QPushButton('Export to CSV')
        self.export_button.clicked.connect(self.export_to_csv)
        results_layout.addWidget(self.export_button)
        
        # Add Run Query and Save Query buttons to results tab
        results_buttons_layout = QHBoxLayout()
        run_query_button_results = QPushButton('Run Query')
        run_query_button_results.clicked.connect(self.query_data_source)
        save_query_button_results = QPushButton('Save Query')
        save_query_button_results.clicked.connect(self.save_query)
        results_buttons_layout.addWidget(run_query_button_results)
        results_buttons_layout.addWidget(save_query_button_results)
        results_layout.addLayout(results_buttons_layout)

        # === SCHEDULE TAB CONTENT ===
        # Schedule tab content
        schedule_form = QFormLayout()

        # Add note about application needing to run
        note_label = QLabel("Note: The application must be running for scheduled tasks to execute.")
        note_label.setStyleSheet("color: red;")
        schedule_layout.addWidget(note_label)

        # Query name
        self.schedule_name_input = QLineEdit()
        schedule_form.addRow("Query Name:", self.schedule_name_input)

        # Output file pattern
        self.output_file_pattern = QLineEdit()
        self.output_file_pattern.setText("{name}_{date}.csv")
        self.output_file_pattern.setToolTip("Available placeholders: {name}, {date}, {time}")
        schedule_form.addRow("Output File Pattern:", self.output_file_pattern)

        # Output directory
        output_dir_layout = QHBoxLayout()
        self.output_dir_input = QLineEdit()
        self.output_dir_input.setText(os.path.expanduser("~"))  # Default to user's home directory
        browse_button = QPushButton("Browse...")
        browse_button.clicked.connect(self.browse_output_dir)
        output_dir_layout.addWidget(self.output_dir_input)
        output_dir_layout.addWidget(browse_button)
        schedule_form.addRow("Output Directory:", output_dir_layout)

        # Schedule frequency
        self.schedule_frequency = QComboBox()
        self.schedule_frequency.addItems(["Daily", "Weekly", "Monthly"])
        self.schedule_frequency.currentIndexChanged.connect(self.update_schedule_options)
        schedule_form.addRow("Frequency:", self.schedule_frequency)

        # Schedule options container (will change based on frequency)
        self.schedule_options_widget = QWidget()
        self.schedule_options_layout = QVBoxLayout(self.schedule_options_widget)
        schedule_form.addRow("Options:", self.schedule_options_widget)

        # Time of day
        time_layout = QHBoxLayout()
        self.schedule_hour = QSpinBox()
        self.schedule_hour.setRange(0, 23)
        self.schedule_hour.setValue(8)  # Default to 8 AM
        self.schedule_minute = QSpinBox()
        self.schedule_minute.setRange(0, 59)
        self.schedule_minute.setValue(0)
        time_layout.addWidget(self.schedule_hour)
        time_layout.addWidget(QLabel(":"))
        time_layout.addWidget(self.schedule_minute)
        time_layout.addStretch(1)
        schedule_form.addRow("Time:", time_layout)

        # Add the form to the layout
        schedule_layout.addLayout(schedule_form)

        # Scheduled Tasks list
        schedule_list_group = QGroupBox("Scheduled Tasks")
        schedule_list_layout = QVBoxLayout()

        self.schedule_list = QTableWidget()
        self.schedule_list.setColumnCount(4)
        self.schedule_list.setHorizontalHeaderLabels(["Name", "Frequency", "Time", "Next Run"])
        self.schedule_list.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.schedule_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.schedule_list.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.schedule_list.horizontalHeader().setStretchLastSection(True)
        self.schedule_list.verticalHeader().setVisible(False)
        self.schedule_list.itemSelectionChanged.connect(self.on_schedule_selected)

        schedule_list_layout.addWidget(self.schedule_list)
        schedule_list_group.setLayout(schedule_list_layout)
        schedule_layout.addWidget(schedule_list_group)

        # Status area
        self.schedule_status = QTextEdit()
        self.schedule_status.setReadOnly(True)
        self.schedule_status.setMaximumHeight(100)  # Limit the height
        schedule_layout.addWidget(QLabel("Status:"))
        schedule_layout.addWidget(self.schedule_status)

        # Buttons
        button_layout = QHBoxLayout()
        self.save_schedule_button = QPushButton("Save Schedule")
        self.save_schedule_button.clicked.connect(self.save_schedule)
        self.remove_schedule_button = QPushButton("Remove Schedule")
        self.remove_schedule_button.clicked.connect(self.remove_schedule)
        debug_button = QPushButton("Test Run (Debug)")
        debug_button.clicked.connect(self.test_scheduled_query)
        button_layout.addWidget(self.save_schedule_button)
        button_layout.addWidget(self.remove_schedule_button)
        button_layout.addWidget(debug_button)
        schedule_layout.addLayout(button_layout)

        # Initialize the schedule options
        self.update_schedule_options(0)  # Default to Daily

        #=== Saved Queries Tab ===#
        # In your initUI method, add a new tab
        self.saved_queries_tab = QWidget()
        saved_queries_layout = QVBoxLayout(self.saved_queries_tab)

        # Add a list widget to display saved queries
        saved_queries_label = QLabel("Saved Queries:")
        self.saved_queries_list = QListWidget()
        self.saved_queries_list.itemDoubleClicked.connect(self.load_saved_query)

        # Add a search box for filtering saved queries
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Search:"))
        self.query_search_input = QLineEdit()
        self.query_search_input.setPlaceholderText("Filter saved queries...")
        self.query_search_input.textChanged.connect(self.filter_saved_queries)
        search_layout.addWidget(self.query_search_input)

        # Add buttons for managing saved queries
        buttons_layout = QHBoxLayout()
        load_button = QPushButton("Load Selected")
        load_button.clicked.connect(self.load_selected_query)
        delete_button = QPushButton("Delete Selected")
        delete_button.clicked.connect(self.delete_selected_query)
        buttons_layout.addWidget(load_button)
        buttons_layout.addWidget(delete_button)

        # Add everything to the layout
        saved_queries_layout.addLayout(search_layout)
        saved_queries_layout.addWidget(saved_queries_label)
        saved_queries_layout.addWidget(self.saved_queries_list)
        saved_queries_layout.addLayout(buttons_layout)

        # Add tabs to tab widget
        self.tab_widget.addTab(self.query_tab, "Query Builder")
        self.tab_widget.addTab(self.filters_tab, "Filters")
        self.tab_widget.addTab(self.results_tab, "Results")
        self.tab_widget.addTab(self.schedule_tab, "Schedule")
        self.tab_widget.addTab(self.saved_queries_tab, "Saved Queries")

        self.tab_widget.currentChanged.connect(self.on_tab_changed)

        main_layout.addWidget(self.tab_widget)
        self.setLayout(main_layout)


    def on_tab_changed(self, index):
        # Load tab-specific resources only when the tab is selected
        if index == 2 and not hasattr(self, 'saved_queries_loaded'):
            self.load_queries_from_disk()
            self.saved_queries_loaded = True

    def update_schedule_options(self, index):
        """Update schedule options based on frequency selection"""
        # Clear existing options
        while self.schedule_options_layout.count():
            item = self.schedule_options_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        
        if index == 0:  # Daily
            # No additional options needed
            pass
        elif index == 1:  # Weekly
            # Day of week selection
            self.day_of_week = QComboBox()
            self.day_of_week.addItems(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
            self.schedule_options_layout.addWidget(QLabel("Day of Week:"))
            self.schedule_options_layout.addWidget(self.day_of_week)
        elif index == 2:  # Monthly
            # Day of month selection
            self.day_of_month = QSpinBox()
            self.day_of_month.setRange(1, 31)
            self.day_of_month.setValue(1)
            self.schedule_options_layout.addWidget(QLabel("Day of Month:"))
            self.schedule_options_layout.addWidget(self.day_of_month)

    def browse_output_dir(self):
        """Open a dialog to select output directory"""
        directory = QFileDialog.getExistingDirectory(self, "Select Output Directory", self.output_dir_input.text())
        if directory:
            self.output_dir_input.setText(directory)

    def execute_query(self, datasource_luid, dimensions, measures, filters):
        """Execute a query with the given parameters"""
        try:
            # Construct the query payload
            fields = [{"fieldCaption": field} for field in dimensions]
            for field, agg in measures:
                fields.append({
                    "fieldCaption": field,
                    "function": agg
                })
            
            # Execute the query
            headers = {
                'X-Tableau-Auth': self.auth_token,
                'Content-Type': 'application/json'
            }
            url = f'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/query-datasource'
            
            payload = {
                "datasource": {
                    "datasourceLuid": datasource_luid
                },
                "query": {
                    "fields": fields,
                    "filters": filters
                }
            }
            
            print(f"Executing query with payload: {payload}")
            
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                return response.json()
            else:
                return f"Error: {response.status_code} - {response.text}"
        except Exception as e:
            import traceback
            traceback.print_exc()
            return f"Error executing query: {str(e)}"

    def update_schedule_display(self):
        """Update the display of scheduled tasks"""
        # Clear the list
        self.schedule_list.setRowCount(0)
        
        if not hasattr(self, 'schedules') or not self.schedules:
            self.schedule_status.setText("No scheduled tasks")
            return
        
        # Get all jobs from the scheduler
        try:
            jobs = self.scheduler.get_jobs()
            job_dict = {job.id: job for job in jobs}
        except Exception as e:
            print(f"Error getting jobs from scheduler: {e}")
            job_dict = {}
        
        # Update column count and headers to include data source
        self.schedule_list.setColumnCount(5)  # Increase to 5 columns
        self.schedule_list.setHorizontalHeaderLabels(["Name", "Data Source", "Frequency", "Time", "Next Run"])
        
        # Add each schedule to the list
        for i, schedule in enumerate(self.schedules):
            self.schedule_list.insertRow(i)
            
            # Name
            self.schedule_list.setItem(i, 0, QTableWidgetItem(schedule["name"]))
            
            # Data Source - Find the name from the LUID
            datasource_name = "Unknown"
            datasource_luid = schedule.get("datasource_luid", "")
            
            # Try to find the data source name from the all_datasources list
            if hasattr(self, 'all_datasources'):
                for name, luid in self.all_datasources:
                    if luid == datasource_luid:
                        datasource_name = name
                        break
            
            self.schedule_list.setItem(i, 1, QTableWidgetItem(datasource_name))
            
            # Frequency
            frequency_text = f"{schedule['frequency']}"
            if schedule['frequency'] == "Weekly" and "day_of_week" in schedule:
                days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                day_index = schedule.get("day_of_week", 0)
                if 0 <= day_index < len(days):
                    frequency_text += f" ({days[day_index]})"
            elif schedule['frequency'] == "Monthly" and "day_of_month" in schedule:
                frequency_text += f" (Day {schedule['day_of_month']})"
            self.schedule_list.setItem(i, 2, QTableWidgetItem(frequency_text))
            
            # Time
            time_text = f"{schedule['hour']:02d}:{schedule['minute']:02d}"
            self.schedule_list.setItem(i, 3, QTableWidgetItem(time_text))
            
            # Next Run
            job_id = f"query_{schedule['name'].replace(' ', '_')}"
            if job_id in job_dict:
                next_run = job_dict[job_id].next_run_time
                next_run_text = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "Not scheduled"
            else:
                next_run_text = "Not scheduled"
            self.schedule_list.setItem(i, 4, QTableWidgetItem(next_run_text))
        
        # Resize columns to content
        self.schedule_list.resizeColumnsToContents()
        
        # Also update the text status area
        status_text = "Scheduled Tasks:\n\n"
        for schedule in self.schedules:
            time_str = f"{schedule['hour']:02d}:{schedule['minute']:02d}"
            datasource_name = "Unknown"
            datasource_luid = schedule.get("datasource_luid", "")
            
            # Try to find the data source name
            if hasattr(self, 'all_datasources'):
                for name, luid in self.all_datasources:
                    if luid == datasource_luid:
                        datasource_name = name
                        break
                        
            status_text += f"• {schedule['name']} ({datasource_name}): {schedule['frequency']}, {schedule.get('detail', '')} at {time_str}\n"
        
        self.schedule_status.setText(status_text)



    def on_schedule_selected(self):
        """Handle selection of a schedule in the list"""
        selected_items = self.schedule_list.selectedItems()
        if not selected_items:
            return
        
        # Get the selected row
        row = selected_items[0].row()
        
        # Get the schedule name from the first column
        schedule_name = self.schedule_list.item(row, 0).text()
        
        # Find the schedule in our list
        for schedule in self.schedules:
            if schedule["name"] == schedule_name:
                # Populate the form with the schedule details
                self.schedule_name_input.setText(schedule["name"])
                self.output_file_pattern.setText(schedule["output_pattern"])
                self.output_dir_input.setText(schedule["output_dir"])
                
                # Set frequency
                index = self.schedule_frequency.findText(schedule["frequency"])
                if index >= 0:
                    self.schedule_frequency.setCurrentIndex(index)
                
                # Set time
                self.schedule_hour.setValue(schedule["hour"])
                self.schedule_minute.setValue(schedule["minute"])
                
                # Set frequency-specific options
                if schedule["frequency"] == "Weekly" and hasattr(self, 'day_of_week'):
                    self.day_of_week.setCurrentIndex(schedule.get("day_of_week", 0))
                elif schedule["frequency"] == "Monthly" and hasattr(self, 'day_of_month'):
                    self.day_of_month.setValue(schedule.get("day_of_month", 1))
                
                break

    def edit_selected_schedule(self):
        """Edit the selected schedule"""
        selected_items = self.schedule_list.selectedItems()
        if not selected_items:
            self.schedule_status.setText("Please select a schedule to edit")
            return
        
        # The form is already populated by on_schedule_selected
        # Just save the schedule with the current form values
        self.save_schedule()

    def remove_selected_schedule(self):
        """Remove the selected schedule"""
        selected_items = self.schedule_list.selectedItems()
        if not selected_items:
            self.schedule_status.setText("Please select a schedule to remove")
            return
        
        # Get the selected row
        row = selected_items[0].row()
        
        # Get the schedule name from the first column
        schedule_name = self.schedule_list.item(row, 0).text()
        
        # Set the name input and call remove_schedule
        self.schedule_name_input.setText(schedule_name)
        self.remove_schedule()

    def test_scheduled_query(self):
        """Test run the current schedule configuration"""
        try:
            name = self.schedule_name_input.text().strip()
            if not name:
                self.schedule_status.setText("Error: Please enter a query name")
                return
                
            output_pattern = self.output_file_pattern.text().strip()
            if not output_pattern:
                self.schedule_status.setText("Error: Please enter an output file pattern")
                return
            
            output_dir = self.output_dir_input.text().strip()
            if not output_dir or not os.path.isdir(output_dir):
                self.schedule_status.setText("Error: Please enter a valid output directory")
                return
                
            # Create a test schedule with current settings
            schedule = {
                "name": name,
                "output_pattern": output_pattern,
                "output_dir": output_dir,
                "datasource_luid": self.current_datasource_luid,
                "dimensions": [item.text() for item in self.dimensions_list.selectedItems()],
                "measures": [(dropdown.currentText(), agg.currentText()) 
                            for dropdown, agg, _ in self.measure_rows 
                            if dropdown.currentText() != "Select Field"],
                "filters": [self.serialize_filter(filter_widget) for filter_widget in self.active_filters]
            }
            
            # For testing, we can use our own execute_query method directly
            self.schedule_status.setText(f"Testing scheduled query '{name}'...")
            
            # Execute the query
            result = self.execute_query(
                datasource_luid=schedule["datasource_luid"],
                dimensions=schedule["dimensions"],
                measures=schedule["measures"],
                filters=schedule["filters"]
            )
            
            # Save the results
            if result and isinstance(result, dict) and 'data' in result:
                # Generate the output filename
                import datetime
                now = datetime.datetime.now()
                date_str = now.strftime("%Y-%m-%d")
                time_str = now.strftime("%H-%M-%S")
                filename = schedule["output_pattern"].format(
                    name=schedule["name"],
                    date=date_str,
                    time=time_str
                )
                
                # Ensure filename ends with .csv
                if not filename.lower().endswith('.csv'):
                    filename += '.csv'
                
                # Create the full path
                output_path = os.path.join(schedule["output_dir"], filename)
                
                # Export the results to CSV
                with open(output_path, 'w', newline='') as csvfile:
                    # Check if data has results
                    results = result.get('data', [])
                    if results:
                        writer = csv.writer(csvfile)
                        # Write headers
                        headers = results[0].keys()
                        writer.writerow(headers)
                        # Write data
                        for row in results:
                            writer.writerow(row.values())
                        
                        self.schedule_status.setText(f"Test completed. Results saved to {output_path}")
                    else:
                        self.schedule_status.setText("Test completed but returned no results")
            else:
                self.schedule_status.setText(f"Test failed: {result}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.schedule_status.setText(f"Error testing schedule: {str(e)}")

    def save_schedule(self):
        """Save the current schedule"""
        try:
            # Validate inputs
            name = self.schedule_name_input.text().strip()
            if not name:
                self.schedule_status.setText("Error: Please enter a query name")
                return
            
            output_pattern = self.output_file_pattern.text().strip()
            if not output_pattern:
                self.schedule_status.setText("Error: Please enter an output file pattern")
                return
            
            output_dir = self.output_dir_input.text().strip()
            if not output_dir or not os.path.isdir(output_dir):
                self.schedule_status.setText("Error: Please enter a valid output directory")
                return
            
            # Get schedule details
            frequency = self.schedule_frequency.currentText()
            hour = self.schedule_hour.value()
            minute = self.schedule_minute.value()

            # Get data source name
            datasource_name = "Unknown"
            if hasattr(self, 'all_datasources'):
                for ds_name, ds_luid in self.all_datasources:
                    if ds_luid == self.current_datasource_luid:
                        datasource_name = ds_name
                        break
            
            # Get frequency-specific options
            if frequency == "Weekly" and hasattr(self, 'day_of_week'):
                day_of_week = self.day_of_week.currentIndex()
                schedule_detail = f"every {self.day_of_week.currentText()}"
            elif frequency == "Monthly" and hasattr(self, 'day_of_month'):
                day_of_month = self.day_of_month.value()
                schedule_detail = f"on day {day_of_month} of each month"
            else:  # Daily
                schedule_detail = "every day"
                day_of_week = None
                day_of_month = None
            
            # Format time
            time_str = f"{hour:02d}:{minute:02d}"
            
            # Create schedule entry
            schedule = {
                "name": name,
                "output_pattern": output_pattern,
                "output_dir": output_dir,
                "frequency": frequency,
                "hour": hour,
                "minute": minute,
                "detail": schedule_detail,
                # Store the current query configuration
                "datasource_luid": self.current_datasource_luid,
                "datasource_name": datasource_name,
                "dimensions": [item.text() for item in self.dimensions_list.selectedItems()],
                "measures": [(dropdown.currentText(), agg.currentText()) 
                            for dropdown, agg, _ in self.measure_rows 
                            if dropdown.currentText() != "Select Field"],
                "filters": [self.serialize_filter(filter_widget) for filter_widget in self.active_filters]
            }
            
            # Add frequency-specific options to the schedule
            if frequency == "Weekly":
                schedule["day_of_week"] = day_of_week
            elif frequency == "Monthly":
                schedule["day_of_month"] = day_of_month
            
            # Initialize schedules list if it doesn't exist
            if not hasattr(self, 'schedules'):
                self.schedules = []
            
            # Check if a schedule with this name already exists
            for i, existing in enumerate(self.schedules):
                if existing["name"] == name:
                    # Replace existing schedule
                    self.schedules[i] = schedule
                    self.schedule_status.setText(f"Updated schedule: {name} will run {schedule_detail} at {time_str}")
                    
                    # Remove the old job if it exists
                    job_id = f"query_{name.replace(' ', '_')}"
                    try:
                        if self.scheduler.get_job(job_id):
                            self.scheduler.remove_job(job_id)
                    except Exception as e:
                        print(f"Error removing existing job: {e}")
                    
                    break
            else:
                # Add new schedule if it doesn't exist
                self.schedules.append(schedule)
                self.schedule_status.setText(f"Added schedule: {name} will run {schedule_detail} at {time_str}")
            
            # Create job ID
            job_id = f"query_{name.replace(' ', '_')}"
            
            # Set up the trigger based on frequency
            if frequency == "Daily":
                self.scheduler.add_job(
                    func=run_scheduled_query_standalone,
                    trigger='cron',
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    name=name,
                    kwargs={'schedule_dict': schedule},
                    replace_existing=True
                )
            elif frequency == "Weekly":
                self.scheduler.add_job(
                    func=run_scheduled_query_standalone,
                    trigger='cron',
                    day_of_week=day_of_week,
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    name=name,
                    kwargs={'schedule_dict': schedule},
                    replace_existing=True
                )
            elif frequency == "Monthly":
                self.scheduler.add_job(
                    func=run_scheduled_query_standalone,
                    trigger='cron',
                    day=day_of_month,
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    name=name,
                    kwargs={'schedule_dict': schedule},
                    replace_existing=True
                )
            
            # Save schedules to disk for persistence
            self.save_schedules_to_disk()
            
            # Update the display
            self.update_schedule_display()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.schedule_status.setText(f"Error creating schedule: {str(e)}")


    def save_schedules_to_disk(self):
        """Save schedules to a JSON file"""
        try:
            # Create a directory for app data if it doesn't exist
            app_dir = os.path.join(os.path.expanduser("~"), ".tableau_query_tool")
            os.makedirs(app_dir, exist_ok=True)
            
            # Save to a JSON file
            schedules_file = os.path.join(app_dir, "saved_schedules.json")
            with open(schedules_file, 'w') as f:
                json.dump(self.schedules, f, indent=2)
                
            print(f"Saved {len(self.schedules)} schedules to {schedules_file}")
        except Exception as e:
            print(f"Error saving schedules to disk: {e}")

    def load_schedules_from_disk(self):
        """Load saved schedules from disk"""
        try:
            app_dir = os.path.join(os.path.expanduser("~"), ".tableau_query_tool")
            schedules_file = os.path.join(app_dir, "saved_schedules.json")
            
            if os.path.exists(schedules_file):
                with open(schedules_file, 'r') as f:
                    self.schedules = json.load(f)
                    
                print(f"Loaded {len(self.schedules)} schedules from disk")
                
                # Re-create scheduler jobs for each schedule
                for schedule in self.schedules:
                    self.recreate_schedule_job(schedule)
                    
                # Update the display
                self.update_schedule_display()
            else:
                self.schedules = []
                print("No saved schedules found")
        except Exception as e:
            print(f"Error loading schedules from disk: {e}")
            self.schedules = []

    def recreate_schedule_job(self, schedule):
        """Recreate a scheduler job from a saved schedule"""
        try:
            name = schedule["name"]
            frequency = schedule["frequency"]
            hour = schedule["hour"]
            minute = schedule["minute"]
            
            # Create job ID
            job_id = f"query_{name.replace(' ', '_')}"
            
            # Set up the trigger based on frequency
            if frequency == "Daily":
                self.scheduler.add_job(
                    func=run_scheduled_query_standalone,
                    trigger='cron',
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    name=name,
                    kwargs={'schedule_dict': schedule},
                    replace_existing=True
                )
            elif frequency == "Weekly" and "day_of_week" in schedule:
                self.scheduler.add_job(
                    func=run_scheduled_query_standalone,
                    trigger='cron',
                    day_of_week=schedule["day_of_week"],
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    name=name,
                    kwargs={'schedule_dict': schedule},
                    replace_existing=True
                )
            elif frequency == "Monthly" and "day_of_month" in schedule:
                self.scheduler.add_job(
                    func=run_scheduled_query_standalone,
                    trigger='cron',
                    day=schedule["day_of_month"],
                    hour=hour,
                    minute=minute,
                    id=job_id,
                    name=name,
                    kwargs={'schedule_dict': schedule},
                    replace_existing=True
                )
            
            print(f"Recreated job for schedule: {name}")
        except Exception as e:
            print(f"Error recreating job for schedule {schedule.get('name', 'unknown')}: {e}")
            import traceback
            traceback.print_exc()

    def serialize_filter(self, filter_widget):
        """Convert a filter widget to a serializable dictionary"""
        # Get the filter dict but don't include any widget references
        filter_dict = filter_widget.get_filter_dict()
        
        # Make a deep copy to avoid modifying the original
        import copy
        serializable_dict = copy.deepcopy(filter_dict)
        
        # Return the serializable version
        return serializable_dict

    def remove_schedule(self):
        """Remove the current schedule"""
        name = self.schedule_name_input.text().strip()
        if not name:
            self.schedule_status.setText("Error: Please enter a query name to remove")
            return
        
        if not hasattr(self, 'schedules'):
            self.schedule_status.setText("No schedules found")
            return
        
        # Show confirmation dialog
        reply = QMessageBox.question(
            self, 
            "Confirm Removal",
            f"Are you sure you want to remove the schedule '{name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return  # User cancelled the removal
        
        # Find and remove the schedule
        for i, schedule in enumerate(self.schedules):
            if schedule["name"] == name:
                self.schedules.pop(i)
                
                # Remove the job from the scheduler
                job_id = f"query_{name.replace(' ', '_')}"
                try:
                    if self.scheduler.get_job(job_id):
                        self.scheduler.remove_job(job_id)
                except Exception as e:
                    print(f"Error removing job: {e}")
                
                # Save the updated schedules to disk
                self.save_schedules_to_disk()
                
                self.schedule_status.setText(f"Removed schedule: {name}")
                self.update_schedule_display()
                return
        
        self.schedule_status.setText(f"No schedule found with name: {name}")

    # def update_schedule_display(self):
    #     """Update the display of scheduled tasks"""
    #     if not hasattr(self, 'schedules') or not self.schedules:
    #         self.schedule_status.setText("No scheduled tasks")
    #         return
        
    #     text = "Scheduled Tasks:\n\n"
    #     for schedule in self.schedules:
    #         time_str = f"{schedule['hour']:02d}:{schedule['minute']:02d}"
    #         text += f"• {schedule['name']}: {schedule['frequency']}, {schedule['detail']} at {time_str}\n"
        
    #     self.schedule_status.setText(text)

    def run_scheduled_query(self, schedule=None):
        """Run a scheduled query"""
        try:
            if schedule is None:
                print("Error: No schedule provided")
                return
                
            print(f"Running scheduled query: {schedule['name']}")
            
            # Set up the query based on the saved configuration
            datasource_luid = schedule["datasource_luid"]
            dimensions = schedule["dimensions"]
            measures = schedule["measures"]
            filters = schedule["filters"]
            
            # Construct the query payload
            fields = [{"fieldCaption": field} for field in dimensions]
            for field, agg in measures:
                fields.append({
                    "fieldCaption": field,
                    "function": agg
                })
            
            # Execute the query
            headers = {
                'X-Tableau-Auth': self.auth_token,
                'Content-Type': 'application/json'
            }
            url = f'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/query-datasource'
            
            payload = {
                "datasource": {
                    "datasourceLuid": datasource_luid
                },
                "query": {
                    "fields": fields,
                    "filters": filters
                }
            }
            
            print(f"Executing query with payload: {payload}")
            
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                
                # Generate the output filename
                import datetime
                now = datetime.datetime.now()
                date_str = now.strftime("%Y-%m-%d")
                time_str = now.strftime("%H-%M-%S")
                filename = schedule["output_pattern"].format(
                    name=schedule["name"],
                    date=date_str,
                    time=time_str
                )
                
                # Ensure filename ends with .csv
                if not filename.lower().endswith('.csv'):
                    filename += '.csv'
                
                # Create the full path
                output_path = os.path.join(schedule["output_dir"], filename)
                
                # Export the results to CSV
                with open(output_path, 'w', newline='') as csvfile:
                    # Check if data has results
                    results = data.get('data', [])
                    if results:
                        writer = csv.writer(csvfile)
                        # Write headers
                        headers = results[0].keys()
                        writer.writerow(headers)
                        # Write data
                        for row in results:
                            writer.writerow(row.values())
                        
                        print(f"Saved results to {output_path}")
                        return f"Query completed successfully. Results saved to {output_path}"
                    else:
                        print(f"Query returned no results")
                        return "Query completed but returned no results"
            else:
                error_msg = f"Query failed with status {response.status_code}: {response.text}"
                print(error_msg)
                return error_msg
                
        except Exception as e:
            error_msg = f"Error running scheduled query '{schedule.get('name', 'unknown')}': {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return error_msg

    def sign_in(self):
        url = 'https://{enter_your_cluster}.online.tableau.com/api/3.25/auth/signin'
        payload = {
            "credentials": {
                "personalAccessTokenName": "{enter_your_token_name}",
                "personalAccessTokenSecret": "{enter_your_token_secret}",
                "site": {
                    "contentUrl": "{enter_your_site_name}"
                }
            }
        }
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            try:
                # Parse the XML response
                root = ET.fromstring(response.text)
                # Extract the token from the XML
                credentials = root.find('.//{http://tableau.com/api}credentials')
                self.auth_token = credentials.attrib['token']
                
                # Extract the site ID
                site_element = root.find('.//{http://tableau.com/api}site')
                if site_element is not None:
                    self.site_id = site_element.attrib.get('id')
                    print(f"Site ID: {self.site_id}")
                else:
                    print("Site element not found in response")
                    self.site_id = None
                    
                print("Successfully signed in. Auth token obtained:", self.auth_token)
                
                # Populate the data source dropdown after successful authentication
                self.populate_datasource_list()
            except ET.ParseError as e:
                self.result_area.setText(f"Error parsing XML: {e}\nResponse text: {response.text}")
        else:
            self.result_area.setText(f'Error signing in: {response.status_code}\n{response.text}')


    def fetch_available_datasources(self):
        if not hasattr(self, 'site_id') or not self.site_id:
            print("No site ID available")
            return []
            
        headers = {
            'X-Tableau-Auth': self.auth_token,
            'Content-Type': 'application/json'
        }
        
        all_datasources = []
        page_num = 1
        page_size = 100  # Default page size in Tableau API
        
        while True:
            url = f'https://{enter_your_cluster}.online.tableau.com/api/3.25/sites/{self.site_id}/datasources?pageSize={page_size}&pageNumber={page_num}'
            
            print(f"Fetching datasources page {page_num} from: {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error fetching page {page_num}: {response.status_code}")
                break
                
            try:
                root = ET.fromstring(response.text)
                
                # Get datasources from this page
                page_datasources = []
                for datasource in root.findall('.//{http://tableau.com/api}datasource'):
                    name = datasource.get('name')
                    luid = datasource.get('id')
                    page_datasources.append((name, luid))
                    
                all_datasources.extend(page_datasources)
                
                # Check if there are more pages
                pagination = root.find('.//{http://tableau.com/api}pagination')
                if pagination is not None:
                    total_available = int(pagination.get('totalAvailable', 0))
                    print(f"Total available: {total_available}, Retrieved so far: {len(all_datasources)}")
                    
                    if len(all_datasources) >= total_available:
                        break  # We've got all datasources
                else:
                    break  # No pagination info, assume we got everything
                    
                page_num += 1
                
            except ET.ParseError as e:
                print(f"XML Parse Error on page {page_num}: {e}")
                break
        
        print(f"Total datasources fetched: {len(all_datasources)}")
        return all_datasources



    def fetch_available_datasources_alternative(self):
        headers = {
            'X-Tableau-Auth': self.auth_token,
            'Content-Type': 'application/json'
        }
        # Try using the VizQL Data Service API instead
        url = 'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/list-datasources'
        
        print(f"Trying alternative datasource fetch from: {url}")
        response = requests.post(url, headers=headers, json={})
        print(f"Alternative datasource fetch status: {response.status_code}")
        print(f"Alternative response: {response.text[:500]}...")
        
        if response.status_code == 200:
            try:
                data = response.json()
                datasources = []
                for ds in data.get('datasources', []):
                    name = ds.get('name', 'Unknown')
                    luid = ds.get('luid')
                    if luid:
                        datasources.append((name, luid))
                return datasources
            except Exception as e:
                self.result_area.setText(f"Error parsing datasources JSON: {e}")
                return []
        else:
            self.result_area.setText(f"Error fetching datasources: {response.status_code}\n{response.text}")
            return []
    
    def populate_datasource_list(self):
        self.datasource_list.clear()
        self.datasource_search.clear()
        
        # Show loading indicator
        self.result_area.setText("Loading data sources...")
        QApplication.processEvents()  # Update the UI
        
        # Get all data sources
        datasources = self.fetch_available_datasources()
        
        # If that doesn't work, try the alternative approach
        if not datasources:
            print("Standard API returned no results, trying alternative...")
            datasources = self.fetch_available_datasources_alternative()
        
        # Sort alphabetically
        datasources.sort(key=lambda x: x[0].lower())
        
        # Store the full list for filtering
        self.all_datasources = datasources
        
        # Add to list widget
        for name, luid in datasources:
            item = QListWidgetItem(name)
            item.setData(Qt.UserRole, luid)  # Store LUID as item data
            self.datasource_list.addItem(item)
        
        if datasources:
            self.result_area.setText(f"Found {len(datasources)} data sources")
        else:
            self.result_area.setText("No data sources found. You can enter a LUID manually below.")
            self.add_manual_luid_input()

    def add_measure_row(self):
        row_layout = QHBoxLayout()
        
        # Field dropdown
        field_dropdown = QComboBox()
        field_dropdown.addItem("Select Measure")
        row_layout.addWidget(field_dropdown)
        
        # Aggregation dropdown
        agg_dropdown = QComboBox()
        agg_dropdown.addItems(['SUM', 'MIN', 'MAX', 'AVG'])
        row_layout.addWidget(agg_dropdown)
        
        # Remove button (except for the first row)
        if len(self.measure_rows) > 0:
            remove_button = QPushButton("X")
            remove_button.setMaximumWidth(30)
            remove_button.clicked.connect(lambda: self.remove_measure_row(row_layout, field_dropdown, agg_dropdown, remove_button))
            row_layout.addWidget(remove_button)
        
        # Store the components
        self.measure_rows.append((field_dropdown, agg_dropdown, row_layout))
        
        # Add to layout
        self.measures_layout.insertLayout(len(self.measure_rows) - 1, row_layout)
        
        # Populate the dropdown with measure fields if we have any
        if len(self.measure_rows) > 1:  # If this isn't the first row
            # Copy items from the first dropdown
            first_dropdown = self.measure_rows[0][0]
            for i in range(first_dropdown.count()):
                field_dropdown.addItem(first_dropdown.itemText(i))


        def remove_measure_row(self, row_layout, field_dropdown, agg_dropdown, remove_button):
            # Find and remove the row from our list first
            row_to_remove = None
            for i, (dropdown, agg, layout) in enumerate(self.measure_rows):
                if layout == row_layout:
                    row_to_remove = i
                    break
            
            if row_to_remove is not None:
                self.measure_rows.pop(row_to_remove)
            
            # Remove widgets from layout and delete them
            field_dropdown.deleteLater()
            agg_dropdown.deleteLater()
            remove_button.deleteLater()
            
            # Schedule the layout for deletion
            row_layout.deleteLater()
            
            # Force update the UI
            QApplication.processEvents()

    def show_add_filter_dialog(self):
        """Show dialog to select a field to filter on"""
        dialog = QDialog(self)
        dialog.setWindowTitle("Add Filter")
        dialog_layout = QVBoxLayout(dialog)
        
        # Field selection
        dialog_layout.addWidget(QLabel("Select Field:"))
        field_list = QListWidget()
        
        # Add all available fields - both dimensions and measures
        # Add dimensions
        for i in range(self.dimensions_list.count()):
            field_list.addItem(self.dimensions_list.item(i).text())
        
        # Add measures
        for dropdown, _, _ in self.measure_rows:
            for i in range(dropdown.count()):
                if i > 0:  # Skip "Select Measure"
                    field_list.addItem(dropdown.itemText(i))
        
        dialog_layout.addWidget(field_list)
        
        # Buttons
        button_layout = QHBoxLayout()
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(dialog.reject)
        add_button = QPushButton("Add")
        add_button.clicked.connect(dialog.accept)
        button_layout.addWidget(cancel_button)
        button_layout.addWidget(add_button)
        dialog_layout.addLayout(button_layout)
        
        if dialog.exec_() == QDialog.Accepted and field_list.currentItem():
            field_name = field_list.currentItem().text()
            self.add_filter(field_name)

    def remove_filter(self, filter_widget):
        """Remove a filter widget"""
        if filter_widget in self.active_filters:
            self.active_filters.remove(filter_widget)
            self.filters_container_layout.removeWidget(filter_widget)
            filter_widget.deleteLater()

    def get_field_type(self, field_name):
        """Get the data type of a field"""
        if hasattr(self, 'field_types') and field_name in self.field_types:
            return self.field_types[field_name]
        return "STRING"  # Default to string if type is unknown

    def filter_datasources(self, search_text):
        self.datasource_list.clear()
        
        search_text = search_text.lower()
        for name, luid in self.all_datasources:
            if search_text in name.lower():
                item = QListWidgetItem(name)
                item.setData(Qt.UserRole, luid)
                self.datasource_list.addItem(item)
    
    def refresh_auth_token(self):
        """Refresh the authentication token to prevent timeouts"""
        print("Refreshing authentication token...")
        try:
            self.sign_in()
            print("Authentication token refreshed successfully")
        except Exception as e:
            print(f"Error refreshing authentication token: {e}")
            import traceback
            traceback.print_exc()

    def on_datasource_selected(self, item):
        name = item.text()
        luid = item.data(Qt.UserRole)
        self.current_datasource_luid = luid
        self.clear_selections()
        self.result_area.setText(f"Selected data source: {name} (LUID: {luid})")

    def add_manual_luid_input(self):
        # Check if we already added the manual input widgets
        if not hasattr(self, 'manual_luid_input'):
            # Add a manual LUID input option
            self.manual_luid_label = QLabel("Enter Data Source LUID manually:")
            self.manual_luid_input = QLineEdit()
            self.manual_luid_button = QPushButton("Use this LUID")
            self.manual_luid_button.clicked.connect(self.use_manual_luid)
            
            # Add these widgets to the layout
            layout = self.layout()
            layout.addWidget(self.manual_luid_label)
            layout.addWidget(self.manual_luid_input)
            layout.addWidget(self.manual_luid_button)


    def use_manual_luid(self):
        luid = self.manual_luid_input.text().strip()
        if luid:
            self.current_datasource_luid = luid
            self.result_area.setText(f"Using manually entered LUID: {luid}")


    def fetch_fields(self):
        if not hasattr(self, 'current_datasource_luid') or not self.current_datasource_luid:
            self.result_area.setText("Please select a data source first")
            return
                
        datasource_luid = self.current_datasource_luid
        
        # Show loading indicator
        self.result_area.setText("Fetching fields, please wait...")
        QApplication.processEvents()
        
        # Try up to 3 times
        for attempt in range(3):
            try:
                headers = {
                    'X-Tableau-Auth': self.auth_token,
                    'Content-Type': 'application/json'
                }
                url = f'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/read-metadata'
                payload = {
                    "datasource": {
                        "datasourceLuid": datasource_luid
                    }
                }

                response = requests.post(url, headers=headers, json=payload)
                print(f"Fetch fields attempt {attempt+1} response status code: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        fields = self.extract_fields(response.json())
                        self.dimensions_list.clear()
                    
                        # Clear all measure dropdowns
                        for dropdown, _, _ in self.measure_rows:
                            dropdown.clear()
                            dropdown.addItem("Select Measure")
                    
                        for field in fields:
                            # Categorize fields as Dimensions or Measures
                            if field['dataType'] in ['STRING', 'DATE', 'BOOLEAN']:
                                self.dimensions_list.addItem(field['fieldName'])
                            elif field['dataType'] in ['INTEGER', 'REAL']:
                                for dropdown, _, _ in self.measure_rows:
                                    dropdown.addItem(field['fieldName'])
                                    
                        self.result_area.setText(f"Fetched {len(fields)} fields successfully")
                        return  # Success, exit the retry loop
                        
                    except ValueError as e:
                        self.result_area.setText(f"Error parsing JSON: {e}\nResponse text: {response.text}")
                
                elif response.status_code == 401:
                    # Authentication error, try to refresh token
                    print("Authentication error, refreshing token...")
                    self.sign_in()
                    # Continue to next attempt
                
                else:
                    self.result_area.setText(f'Error: {response.status_code}\n{response.text}')
                    
                # Wait before retrying
                if attempt < 2:  # Don't wait after the last attempt
                    import time
                    time.sleep(2)
                    
            except Exception as e:
                print(f"Error in fetch_fields attempt {attempt+1}: {e}")
                import traceback
                traceback.print_exc()
                
                # Wait before retrying
                if attempt < 2:  # Don't wait after the last attempt
                    import time
                    time.sleep(2)
        
        # If we get here, all attempts failed
        self.result_area.setText("Failed to fetch fields after multiple attempts. Please try again later.")

    
    def filter_datasources(self, search_text):
        self.datasource_list.clear()
        search_text = search_text.lower()
        for name, luid in self.all_datasources:
            if search_text in name.lower():
                item = QListWidgetItem(name)
                item.setData(Qt.UserRole, luid)
                self.datasource_list.addItem(item)

    def on_datasource_selected(self, item):
        name = item.text()
        luid = item.data(Qt.UserRole)
        self.current_datasource_luid = luid
        self.clear_selections()
        self.result_area.setText(f"Selected data source: {name} (LUID: {luid})")

    def add_manual_luid_input(self):
        # Check if we already added the manual input widgets
        if not hasattr(self, 'manual_luid_input'):
            # Add a manual LUID input option
            self.manual_luid_label = QLabel("Enter Data Source LUID manually:")
            self.manual_luid_input = QLineEdit()
            self.manual_luid_button = QPushButton("Use this LUID")
            self.manual_luid_button.clicked.connect(self.use_manual_luid)
            
            # Add these widgets to the layout
            layout = self.layout()
            layout.addWidget(self.manual_luid_label)
            layout.addWidget(self.manual_luid_input)
            layout.addWidget(self.manual_luid_button)


    def extract_fields(self, metadata):
        fields = []
        # Store field types in a dictionary for quick lookup
        self.field_types = {}
        
        for field in metadata.get('data', []):
            field_name = field['fieldName']
            data_type = field['dataType']
            
            fields.append({
                'fieldName': field_name,
                'dataType': data_type
            })
            
            # Store the field type
            self.field_types[field_name] = data_type
        
        print("Extracted fields:", fields)  # Debugging output
        return fields

    def update_selected_dimensions_display(self):
        selected_dimensions = [item.text() for item in self.dimensions_list.selectedItems()][:10]
        # Update the selected dimensions display
        self.selected_dimensions_display.setText("\n".join(selected_dimensions))

    def clear_selections(self):
        # Clear selections and displays when data source changes
        self.dimensions_list.clearSelection()
        self.selected_dimensions_display.clear()
        
        # Clear filter section
        # Remove this line: self.filter_field_dropdown.clear()
        # Remove this line: self.filter_value_input.clear()
        
        # Clear all existing filters
        for filter_widget in self.active_filters[:]:  # Make a copy of the list to safely iterate
            self.remove_filter(filter_widget)
        
        # Clear measure selections
        for dropdown, agg, _ in self.measure_rows:
            dropdown.setCurrentIndex(0)

    def save_query(self):
        """Save the current query configuration"""
        # Check if a data source is selected
        if not hasattr(self, 'current_datasource_luid') or not self.current_datasource_luid:
            self.result_area.setText("Please select a data source before saving the query")
            return
        
        # Get the current data source name
        datasource_name = ""
        for i in range(self.datasource_list.count()):
            item = self.datasource_list.item(i)
            if item.data(Qt.UserRole) == self.current_datasource_luid:
                datasource_name = item.text()
                break
        
        # Prompt for a query name
        query_name, ok = QInputDialog.getText(self, "Save Query", "Enter a name for this query:")
        if not ok or not query_name.strip():
            return  # User cancelled or entered an empty name
        
        query_name = query_name.strip()
        
        # Create a query object
        query = {
            "name": query_name,
            "datasource_name": datasource_name,
            "datasource_luid": self.current_datasource_luid,
            "dimensions": [item.text() for item in self.dimensions_list.selectedItems()],
            "measures": [(dropdown.currentText(), agg.currentText()) 
                        for dropdown, agg, _ in self.measure_rows 
                        if dropdown.currentText() != "Select Field"],
            "filters": [filter_widget.get_filter_dict() for filter_widget in self.active_filters],
            "date_saved": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Initialize saved_queries if it doesn't exist
        if not hasattr(self, 'saved_queries'):
            self.saved_queries = []
        
        # Check if a query with this name already exists
        for i, existing in enumerate(self.saved_queries):
            if existing["name"] == query_name:
                # Ask for confirmation to overwrite
                reply = QMessageBox.question(self, "Confirm Overwrite", 
                                            f"A query named '{query_name}' already exists. Overwrite it?",
                                            QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                if reply == QMessageBox.Yes:
                    # Replace the existing query
                    self.saved_queries[i] = query
                    self.result_area.setText(f"Query '{query_name}' updated")
                    self.update_saved_queries_list()
                    return
                else:
                    return  # User cancelled overwrite
        
        # Add the new query
        self.saved_queries.append(query)
        self.result_area.setText(f"Query '{query_name}' saved")
        
        # Save to disk (optional)
        self.save_queries_to_disk()
        
        # Update the list
        self.update_saved_queries_list()

    def update_saved_queries_list(self):
        """Update the list of saved queries"""
        self.saved_queries_list.clear()
        
        if not hasattr(self, 'saved_queries') or not self.saved_queries:
            return
        
        # Sort queries by name
        sorted_queries = sorted(self.saved_queries, key=lambda q: q["name"].lower())
        
        for query in sorted_queries:
            # Create a display string with name and data source
            display_text = f"{query['name']} ({query['datasource_name']})"
            
            # Add to list with the query object as data
            item = QListWidgetItem(display_text)
            item.setData(Qt.UserRole, query)
            self.saved_queries_list.addItem(item)

    def filter_saved_queries(self, text):
        """Filter the saved queries list based on search text"""
        search_text = text.lower()
        
        for i in range(self.saved_queries_list.count()):
            item = self.saved_queries_list.item(i)
            query = item.data(Qt.UserRole)
            
            # Check if search text is in query name or data source name
            if (search_text in query["name"].lower() or 
                search_text in query["datasource_name"].lower()):
                item.setHidden(False)
            else:
                item.setHidden(True)

    def load_saved_query(self, item):
        """Load a saved query when double-clicked"""
        query = item.data(Qt.UserRole)
        self.apply_saved_query(query)

    def load_selected_query(self):
        """Load the selected query"""
        selected_items = self.saved_queries_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self, "No Selection", "Please select a query to load")
            return
        
        query = selected_items[0].data(Qt.UserRole)
        self.apply_saved_query(query)

    def delete_selected_query(self):
        """Delete the selected query"""
        selected_items = self.saved_queries_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self, "No Selection", "Please select a query to delete")
            return
        
        query = selected_items[0].data(Qt.UserRole)
        
        # Confirm deletion
        reply = QMessageBox.question(self, "Confirm Deletion", 
                                    f"Are you sure you want to delete the query '{query['name']}'?",
                                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply != QMessageBox.Yes:
            return
        
        # Remove from the list
        for i, saved_query in enumerate(self.saved_queries):
            if saved_query["name"] == query["name"]:
                self.saved_queries.pop(i)
                break
        
        # Save to disk
        self.save_queries_to_disk()
        
        # Update the list
        self.update_saved_queries_list()
        
        self.result_area.setText(f"Query '{query['name']}' deleted")

    def apply_saved_query(self, query):
        """Apply a saved query to the current UI"""
        # Reset current selections
        self.reset_selections()
        
        # Set the data source
        datasource_luid = query["datasource_luid"]
        self.current_datasource_luid = datasource_luid
        
        # Find and select the data source in the list
        for i in range(self.datasource_list.count()):
            item = self.datasource_list.item(i)
            if item.data(Qt.UserRole) == datasource_luid:
                self.datasource_list.setCurrentItem(item)
                break
        
        # Fetch fields for this data source
        self.fetch_fields()
        
        # Store the query to apply after fields are loaded
        self.query_to_apply = query
        
        # Wait a moment for fields to load
        QTimer.singleShot(1000, self.apply_saved_query_after_fetch)

    def apply_saved_query_after_fetch(self):
        """Apply the saved query after fields have been fetched"""
        if not hasattr(self, 'query_to_apply'):
            return
            
        query = self.query_to_apply
        
        # Select dimensions
        for dim in query["dimensions"]:
            for i in range(self.dimensions_list.count()):
                if self.dimensions_list.item(i).text() == dim:
                    self.dimensions_list.item(i).setSelected(True)
        
        # Set up measures
        # First, ensure we have enough measure rows
        while len(self.measure_rows) < len(query["measures"]):
            self.add_measure_row()
        
        # Then set the values
        for i, (field, agg) in enumerate(query["measures"]):
            dropdown, agg_dropdown, _ = self.measure_rows[i]
            
            # Set field
            for j in range(dropdown.count()):
                if dropdown.itemText(j) == field:
                    dropdown.setCurrentIndex(j)
                    break
            
            # Set aggregation
            for j in range(agg_dropdown.count()):
                if agg_dropdown.itemText(j) == agg:
                    agg_dropdown.setCurrentIndex(j)
                    break
        
        # Clear existing filters
        for filter_widget in self.active_filters[:]:
            self.remove_filter(filter_widget)
        
        # Recreate filters
        if "filters" in query and query["filters"]:
            print(f"Attempting to recreate {len(query['filters'])} filters")
            
            # Switch to the Filters tab to make filters visible
            self.tab_widget.setCurrentIndex(1)
            
            for filter_dict in query["filters"]:
                try:
                    # Extract field name from the filter
                    field_name = filter_dict.get("field", {}).get("fieldCaption", "")
                    if not field_name:
                        print(f"Skipping filter - no field name found")
                        continue
                    
                    # Determine filter type
                    filter_type = filter_dict.get("filterType", "")
                    
                    # Create the filter widget
                    filter_widget = None
                    if filter_type == "QUANTITATIVE_DATE" or filter_type == "DATE":
                        filter_widget = DateFilterWidget(field_name, self)
                    elif filter_type == "QUANTITATIVE_NUMERICAL":
                        filter_widget = NumberFilterWidget(field_name, self)
                    elif filter_type == "SET":
                        filter_widget = StringFilterWidget(field_name, self)
                    else:
                        print(f"Unknown filter type: {filter_type}")
                        continue
                    
                    # Connect remove signal
                    filter_widget.removed.connect(self.remove_filter)
                    
                    # Add to UI and track
                    self.filters_container_layout.insertWidget(len(self.active_filters), filter_widget)
                    self.active_filters.append(filter_widget)
                    
                    # Configure the filter widget
                    self.configure_filter_widget(filter_widget, filter_dict)
                    
                    print(f"Added filter for {field_name} of type {filter_type}")
                except Exception as e:
                    print(f"Error recreating filter: {e}")
                    import traceback
                    traceback.print_exc()
        
        # Force update the UI
        QApplication.processEvents()
        
        self.result_area.setText(f"Query '{query['name']}' loaded with {len(query.get('filters', []))} filters")
        
        # Clean up
        delattr(self, 'query_to_apply')


    def add_filter(self, field_name, field_type=None):
        """Add a filter for the selected field"""
        # If field_type is not provided, determine it
        if field_type is None:
            field_type = self.get_field_type(field_name)
        
        # Create appropriate filter widget
        if field_type == "DATE":
            filter_widget = DateFilterWidget(field_name, self)
        elif field_type in ["INTEGER", "REAL", "NUMBER"]:
            filter_widget = NumberFilterWidget(field_name, self)
        else:  # Default to string filter
            filter_widget = StringFilterWidget(field_name, self)
        
        # Connect remove signal
        filter_widget.removed.connect(self.remove_filter)
        
        # Add to UI and track
        self.filters_container_layout.insertWidget(len(self.active_filters), filter_widget)
        
        # Add some spacing between filters
        spacer = QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.filters_container_layout.insertSpacerItem(len(self.active_filters) + 1, spacer)
        
        self.active_filters.append(filter_widget)
        
        return filter_widget

    def configure_filter_widget(self, filter_widget, filter_dict):
        """Configure a filter widget based on a filter dictionary"""
        try:
            print(f"Configuring filter widget for {filter_widget.field_name} with {filter_dict}")
            
            if isinstance(filter_widget, DateFilterWidget):
                # Configure date filter
                filter_type = filter_dict.get("filterType", "")
                
                if filter_type == "QUANTITATIVE_DATE":
                    filter_widget.filter_type_combo.setCurrentIndex(0)  # Quantitative
                    
                    quant_type = filter_dict.get("quantitativeFilterType", "")
                    if quant_type == "RANGE":
                        filter_widget.quant_type_combo.setCurrentIndex(0)  # Range
                        if "minDate" in filter_dict:
                            min_date = QDate.fromString(filter_dict["minDate"], Qt.ISODate)
                            filter_widget.from_date.setDate(min_date)
                        if "maxDate" in filter_dict:
                            max_date = QDate.fromString(filter_dict["maxDate"], Qt.ISODate)
                            filter_widget.to_date.setDate(max_date)
                    elif quant_type == "MIN":
                        filter_widget.quant_type_combo.setCurrentIndex(1)  # Min Date
                        if "minDate" in filter_dict:
                            min_date = QDate.fromString(filter_dict["minDate"], Qt.ISODate)
                            filter_widget.min_only_date.setDate(min_date)
                    elif quant_type == "MAX":
                        filter_widget.quant_type_combo.setCurrentIndex(2)  # Max Date
                        if "maxDate" in filter_dict:
                            max_date = QDate.fromString(filter_dict["maxDate"], Qt.ISODate)
                            filter_widget.max_only_date.setDate(max_date)
                    elif quant_type == "ONLY_NULL":
                        filter_widget.quant_type_combo.setCurrentIndex(3)  # Only Null
                    elif quant_type == "ONLY_NON_NULL":
                        filter_widget.quant_type_combo.setCurrentIndex(4)  # Only Non-Null
                
                elif filter_type == "DATE":
                    filter_widget.filter_type_combo.setCurrentIndex(1)  # Relative
                    
                    # Set period type
                    period_type = filter_dict.get("periodType", "DAYS")
                    for i in range(filter_widget.period_type_combo.count()):
                        if filter_widget.period_type_combo.itemText(i) == period_type:
                            filter_widget.period_type_combo.setCurrentIndex(i)
                            break
                    
                    # Set date range type
                    date_range_type = filter_dict.get("dateRangeType", "LAST")
                    for i in range(filter_widget.date_range_type_combo.count()):
                        if filter_widget.date_range_type_combo.itemText(i) == date_range_type:
                            filter_widget.date_range_type_combo.setCurrentIndex(i)
                            break
                    
                    # Set range N if applicable
                    if "rangeN" in filter_dict and hasattr(filter_widget, 'range_n_input'):
                        filter_widget.range_n_input.setValue(filter_dict["rangeN"])
                        
            elif isinstance(filter_widget, NumberFilterWidget):
                # Configure number filter
                quant_type = filter_dict.get("quantitativeFilterType", "")
                
                if quant_type == "RANGE":
                    filter_widget.filter_type_combo.setCurrentIndex(0)  # Range
                    if "min" in filter_dict:
                        filter_widget.min_input.setText(str(filter_dict["min"]))
                    if "max" in filter_dict:
                        filter_widget.max_input.setText(str(filter_dict["max"]))
                elif quant_type == "MIN":
                    filter_widget.filter_type_combo.setCurrentIndex(1)  # Min Only
                    if "min" in filter_dict:
                        filter_widget.min_only_input.setText(str(filter_dict["min"]))
                elif quant_type == "MAX":
                    filter_widget.filter_type_combo.setCurrentIndex(2)  # Max Only
                    if "max" in filter_dict:
                        filter_widget.max_only_input.setText(str(filter_dict["max"]))
                elif quant_type == "ONLY_NULL":
                    filter_widget.filter_type_combo.setCurrentIndex(3)  # Only Null
                elif quant_type == "ONLY_NON_NULL":
                    filter_widget.filter_type_combo.setCurrentIndex(4)  # Only Non-Null
                
                # Set function if present
                if "field" in filter_dict and "function" in filter_dict["field"] and hasattr(filter_widget, 'function_combo'):
                    function = filter_dict["field"]["function"]
                    for i in range(filter_widget.function_combo.count()):
                        if filter_widget.function_combo.itemText(i) == function:
                            filter_widget.function_combo.setCurrentIndex(i)
                            break
                            
            elif isinstance(filter_widget, StringFilterWidget):
                # Configure string filter
                if "values" in filter_dict:
                    # Store the values to select after fetching
                    filter_widget.values_to_select = filter_dict["values"]
                    
                    # Fetch available values
                    if hasattr(filter_widget, 'fetch_available_values'):
                        filter_widget.fetch_available_values()
                        
                        # Wait a moment for values to load, then select them
                        QTimer.singleShot(1000, lambda: self.select_filter_values(filter_widget))
        
        except Exception as e:
            print(f"Error configuring filter widget: {e}")
            import traceback
            traceback.print_exc()

    def select_filter_values(self, filter_widget):
        """Select values in a filter widget's list"""
        if hasattr(filter_widget, 'values_list') and hasattr(filter_widget, 'values_to_select'):
            values_to_select = filter_widget.values_to_select
            
            for i in range(filter_widget.values_list.count()):
                item = filter_widget.values_list.item(i)
                if item.text() in values_to_select:
                    item.setSelected(True)
            
            # Clean up
            delattr(filter_widget, 'values_to_select')

    # def select_filter_values(self, filter_widget, values):
    #     """Select values in a filter widget's list"""
    #     if hasattr(filter_widget, 'values_list'):
    #         for i in range(filter_widget.values_list.count()):
    #             item = filter_widget.values_list.item(i)
    #             if item.text() in values:
    #                 item.setSelected(True)

    def save_queries_to_disk(self):
        """Save queries to a JSON file"""
        try:
            import json
            
            # Create a directory for app data if it doesn't exist
            app_dir = os.path.join(os.path.expanduser("~"), ".tableau_query_tool")
            os.makedirs(app_dir, exist_ok=True)
            
            # Save to a JSON file
            queries_file = os.path.join(app_dir, "saved_queries.json")
            with open(queries_file, 'w') as f:
                json.dump(self.saved_queries, f, indent=2)
                
        except Exception as e:
            print(f"Error saving queries to disk: {e}")

    def load_queries_from_disk(self):
        """Load saved queries from disk"""
        try:
            import json
            
            queries_file = os.path.join(os.path.expanduser("~"), ".tableau_query_tool", "saved_queries.json")
            if os.path.exists(queries_file):
                with open(queries_file, 'r') as f:
                    self.saved_queries = json.load(f)
                    self.update_saved_queries_list()
                    
        except Exception as e:
            print(f"Error loading queries from disk: {e}")

    def query_data_source(self):
        try:
            if not hasattr(self, 'current_datasource_luid') or not self.current_datasource_luid:
                self.result_area.setText("Please select a data source first")
                return
                
            datasource_luid = self.current_datasource_luid
            selected_dimensions = [item.text() for item in self.dimensions_list.selectedItems()][:10]
            
            # Construct the payload with selected fields
            fields = [{"fieldCaption": field} for field in selected_dimensions]
            
            # Add measures with aggregations
            for dropdown, agg_dropdown, _ in self.measure_rows:
                field = dropdown.currentText()
                if field != "Select Measure":
                    aggregation_function = agg_dropdown.currentText()
                    fields.append({
                        "fieldCaption": field,
                        "function": aggregation_function
                    })

            # Get filters from filter widgets
            filters = []
            for i, filter_widget in enumerate(self.active_filters):
                filter_dict = filter_widget.get_filter_dict()
                print(f"Filter {i} for {filter_widget.field_name}: {filter_dict}")
                filters.append(filter_dict)
            
            # Show that query is running
            self.result_area.setText("Query running, please wait...")
            
            # Disable the query button if it exists
            if hasattr(self, 'query_button') and self.query_button is not None:
                self.query_button.setEnabled(False)
            
            # Define URL and headers
            url = 'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/query-datasource'
            headers = {
                'X-Tableau-Auth': self.auth_token,
                'Content-Type': 'application/json'
            }
            
            payload = {
                "datasource": {
                    "datasourceLuid": datasource_luid
                },
                "query": {
                    "fields": fields,
                    "filters": filters
                }
            }

            print(f"Final payload: {payload}")

            response = requests.post(url, headers=headers, json=payload)
            print("Query response status code:", response.status_code)
            print("Query response text:", response.text)  # Debugging output

            if response.status_code == 200:
                # Display the raw JSON response
                self.result_area.setText(response.text)
                self.display_results(response.json())  # Display the results in a table
                
                # Switch to the results tab
                self.tab_widget.setCurrentIndex(2)  # Assuming results tab is index 2
            else:
                self.result_area.setText(f'Error: {response.status_code}\n{response.text}')
        except Exception as e:
            self.result_area.setText(f"An error occurred: {e}")
        finally:
            # Re-enable the query button if it exists
            if hasattr(self, 'query_button') and self.query_button is not None:
                self.query_button.setEnabled(True)


    def handle_query_result(self, data):
        # Display the raw JSON response
        self.result_area.setText(str(data))
        self.display_results(data)  # Display the results in a table

    def handle_query_error(self, error_message):
        self.result_area.setText(error_message)

    def query_finished(self):
        self.query_button.setEnabled(True)
        self.cancel_button.setEnabled(False)

    def cancel_query(self):
        if hasattr(self, 'query_worker') and self.query_worker.isRunning():
            self.query_worker.cancel()
            self.result_area.setText("Query cancelled")
            self.query_button.setEnabled(True)
            self.cancel_button.setEnabled(False)

    def reset_selections(self):
        """Reset all selections and filters"""
        # Clear dimension selections
        self.dimensions_list.clearSelection()
        self.selected_dimensions_display.clear()
        
        # Clear measure selections
        for dropdown, agg_dropdown, _ in self.measure_rows:
            dropdown.setCurrentIndex(0)
        
        # Remove all measure rows except the first one
        while len(self.measure_rows) > 1:
            # Get the last row
            _, _, layout = self.measure_rows[-1]
            
            # Remove the row from our list
            self.measure_rows.pop()
            
            # Remove the layout and its widgets
            while layout.count():
                item = layout.takeAt(0)
                widget = item.widget()
                if widget:
                    widget.deleteLater()
            
            # Remove the layout itself
            self.measures_layout.removeItem(layout)
        
        # Clear all filters
        for filter_widget in self.active_filters[:]:  # Make a copy of the list to safely iterate
            self.remove_filter(filter_widget)
        
        # Update the UI
        self.result_area.setText("All selections and filters have been reset.")
        
        # Switch back to the Query Builder tab
        self.tab_widget.setCurrentIndex(0)


    def display_results(self, data):
        # Assuming data is a dictionary with a 'data' key containing a list of records
        results = data.get('data', [])
        if results:
            # Assuming each record is a dictionary with keys as column names
            self.result_table.setRowCount(len(results))
            self.result_table.setColumnCount(len(results[0]))
            self.result_table.setHorizontalHeaderLabels(results[0].keys())
            for row_idx, row_data in enumerate(results):
                for col_idx, (key, value) in enumerate(row_data.items()):
                    self.result_table.setItem(row_idx, col_idx, QTableWidgetItem(str(value)))
        else:
            self.result_area.setText("No results found.")

    def export_to_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save CSV", "", "CSV Files (*.csv)")
        if path:
            with open(path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                # Write headers
                headers = [self.result_table.horizontalHeaderItem(i).text() for i in range(self.result_table.columnCount())]
                writer.writerow(headers)
                # Write data
                for row in range(self.result_table.rowCount()):
                    row_data = [self.result_table.item(row, col).text() for col in range(self.result_table.columnCount())]
                    writer.writerow(row_data)

class FilterWidget(QWidget):
    removed = pyqtSignal(object)  # Signal when filter is removed
    
    def __init__(self, field_name, field_type, parent=None):
        super().__init__(parent)
        self.field_name = field_name
        self.field_type = field_type
        self.setup_ui()
        # Set a better size policy
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        # Set a reasonable fixed height based on the filter type
        if field_type == "DATE":
            self.setMinimumHeight(200)  # Date filters need more space
            self.setMaximumHeight(250)
        elif field_type == "STRING":
            self.setMinimumHeight(250)  # String filters with lists need more space
            self.setMaximumHeight(350)
        else:
            self.setMinimumHeight(150)  # Number filters need less space
            self.setMaximumHeight(200)
        
        # Add a frame/border to visually separate filters
        self.setStyleSheet("""
            FilterWidget {
                border: 1px solid #cccccc;
                border-radius: 5px;
                background-color: #f9f9f9;
                margin: 5px;
            }
        """)
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)  # Reduce margins
        layout.setSpacing(5)  # Reduce spacing
        
        # Header with field name and remove button
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(0, 0, 0, 0)  # No margins for header
        header_layout.setSpacing(5)  # Reduce spacing
        
        self.field_label = QLabel(self.field_name)
        self.field_label.setStyleSheet("font-weight: bold;")
        header_layout.addWidget(self.field_label)
        
        self.remove_button = QPushButton("✕")  # Use a nicer X character
        self.remove_button.setMaximumWidth(24)
        self.remove_button.setMaximumHeight(24)
        self.remove_button.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border-radius: 12px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #d32f2f;
            }
        """)
        self.remove_button.clicked.connect(self.remove_filter)
        header_layout.addWidget(self.remove_button)
        
        layout.addLayout(header_layout)
        
        # Add a separator line
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line)
        
        # Content area - to be filled by subclasses
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0, 5, 0, 0)  # Reduce margins
        self.content_layout.setSpacing(5)  # Reduce spacing
        layout.addWidget(self.content_widget)

    
    def remove_filter(self):
        self.removed.emit(self)
    
    def get_filter_dict(self):
        # Base method to be overridden by subclasses
        return {
            "field": {
                "fieldCaption": self.field_name
            }
        }

class StringFilterWidget(FilterWidget):
    def __init__(self, field_name, parent=None):
        super().__init__(field_name, "STRING", parent)
        self.main_app = self.find_main_app(parent)
        self.values_to_select = []  # Add this line to store values to select
        
    def find_main_app(self, widget):
        """Find the main TableauApp instance by traversing up the parent hierarchy"""
        if widget is None:
            return None
        if isinstance(widget, TableauApp):
            return widget
        return self.find_main_app(widget.parent())
        
    def setup_ui(self):
        super().setup_ui()
        
        # Create layout for the filter controls
        filter_controls_layout = QVBoxLayout()
        
        # Add a label
        filter_controls_layout.addWidget(QLabel("Select values:"))
        
        # Create a multi-select list widget with limited height
        self.values_list = QListWidget()
        self.values_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self.values_list.setMaximumHeight(150)  # Limit the height
        filter_controls_layout.addWidget(self.values_list)
        
        # Add a refresh button to fetch values
        refresh_button = QPushButton("Fetch Available Values")
        refresh_button.clicked.connect(self.fetch_available_values)
        filter_controls_layout.addWidget(refresh_button)
        
        # Add a search box for filtering the list
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search values...")
        self.search_input.textChanged.connect(self.filter_values)
        filter_controls_layout.addWidget(self.search_input)
        
        self.content_layout.addLayout(filter_controls_layout)
    
    def filter_values(self, text):
        """Filter the values list based on search text"""
        if hasattr(self, 'all_values'):
            self.values_list.clear()
            search_text = text.lower()
            for value in self.all_values:
                if search_text in value.lower():
                    self.values_list.addItem(value)
    
    def fetch_available_values(self):
        """Fetch available values for this field from the data source"""
        # Add debug output
        print("Fetching available values...")
        
        # Check if we have access to the main app
        if self.main_app is None:
            print("Error: Cannot access main application")
            QMessageBox.warning(self, "Error", "Cannot access main application")
            return
            
        # Check if we have the required authentication and datasource info
        if not hasattr(self.main_app, 'auth_token') or not self.main_app.auth_token:
            print("Error: No auth token available")
            QMessageBox.warning(self, "Error", "No authentication token available. Please sign in again.")
            return
            
        if not hasattr(self.main_app, 'current_datasource_luid') or not self.main_app.current_datasource_luid:
            print("Error: No datasource LUID available")
            QMessageBox.warning(self, "Error", "No data source selected. Please select a data source first.")
            return
            
        auth_token = self.main_app.auth_token
        datasource_luid = self.main_app.current_datasource_luid
        
        print(f"Using auth token: {auth_token[:10]}... and datasource LUID: {datasource_luid}")
        
        # Show a loading indicator
        self.values_list.clear()
        self.values_list.addItem("Loading values...")
        QApplication.processEvents()  # Update the UI
        
        headers = {
            'X-Tableau-Auth': auth_token,
            'Content-Type': 'application/json'
        }
        url = f'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/query-datasource'
        
        # Create a query that just returns distinct values for this field
        payload = {
            "datasource": {
                "datasourceLuid": datasource_luid
            },
            "query": {
                "fields": [{"fieldCaption": self.field_name}]
            }
        }
        
        print(f"Sending request with payload: {payload}")
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            print(f"Response status code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"Received data: {str(data)[:100]}...")  # Print first 100 chars
                
                # Extract unique values from the response
                unique_values = set()
                for row in data.get('data', []):
                    value = row.get(self.field_name)
                    if value is not None:
                        unique_values.add(str(value))
                
                print(f"Found {len(unique_values)} unique values")
                
                # Store all values for filtering
                self.all_values = sorted(list(unique_values))
                
                # Populate the list widget
                self.values_list.clear()
                for value in self.all_values:
                    self.values_list.addItem(value)
                    
                print("List widget populated with values")
                
                # If we have values to select, select them
                if hasattr(self, 'values_to_select') and self.values_to_select:
                    for i in range(self.values_list.count()):
                        item = self.values_list.item(i)
                        if item.text() in self.values_to_select:
                            item.setSelected(True)
            
            elif response.status_code == 401:
                # Authentication error
                error_msg = "Authentication error. Please sign in again."
                print(error_msg)
                QMessageBox.warning(self, "Authentication Error", error_msg)
                # Try to refresh the token
                if hasattr(self.main_app, 'sign_in'):
                    self.main_app.sign_in()
            else:
                error_msg = f"Error fetching values: {response.status_code}\n{response.text}"
                print(error_msg)
                QMessageBox.warning(self, "Error", f"Failed to fetch values: {response.status_code}")
                # Show error in the UI
                if hasattr(self.main_app, 'result_area'):
                    self.main_app.result_area.setText(error_msg)
        except Exception as e:
            error_msg = f"Error fetching values: {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            QMessageBox.warning(self, "Error", f"Exception while fetching values: {str(e)}")
            # Show error in the UI
            if hasattr(self.main_app, 'result_area'):
                self.main_app.result_area.setText(error_msg)
        finally:
            # If we failed to get any values, show a message
            if not hasattr(self, 'all_values') or not self.all_values:
                self.values_list.clear()
                self.values_list.addItem("No values found or error occurred")

    
    def get_filter_dict(self):
        filter_dict = {
            "filterType": "SET",
            "field": {
                "fieldCaption": self.field_name
            },
            "exclude": False,
            "values": []
        }
        
        # Get selected values from the list widget
        for item in self.values_list.selectedItems():
            filter_dict["values"].append(item.text())
            
        return filter_dict

class NumberFilterWidget(FilterWidget):
    def __init__(self, field_name, parent=None):
        super().__init__(field_name, "NUMBER", parent)
        
    def setup_ui(self):
        super().setup_ui()
        
        # Filter type selection
        self.filter_type_combo = QComboBox()
        self.filter_type_combo.addItems(["Range", "Min Only", "Max Only", "Only Null", "Only Non-Null"])
        self.filter_type_combo.currentIndexChanged.connect(self.update_filter_controls)
        self.content_layout.addWidget(QLabel("Filter Type:"))
        self.content_layout.addWidget(self.filter_type_combo)
        
        # Function selection for measures
        self.function_combo = QComboBox()
        self.function_combo.addItems(["SUM", "AVG", "MIN", "MAX", "COUNT"])
        self.content_layout.addWidget(QLabel("Aggregation:"))
        self.content_layout.addWidget(self.function_combo)
        
        # Stacked widget for different filter types
        self.filter_stack = QStackedWidget()
        
        # Range controls
        range_widget = QWidget()
        range_layout = QVBoxLayout(range_widget)
        
        min_layout = QHBoxLayout()
        min_layout.addWidget(QLabel("Min:"))
        self.min_input = QLineEdit()
        self.min_input.setValidator(QDoubleValidator())
        min_layout.addWidget(self.min_input)
        range_layout.addLayout(min_layout)
        
        max_layout = QHBoxLayout()
        max_layout.addWidget(QLabel("Max:"))
        self.max_input = QLineEdit()
        self.max_input.setValidator(QDoubleValidator())
        max_layout.addWidget(self.max_input)
        range_layout.addLayout(max_layout)
        
        # Min only controls
        min_only_widget = QWidget()
        min_only_layout = QVBoxLayout(min_only_widget)
        min_only_layout.addWidget(QLabel("Min Value:"))
        self.min_only_input = QLineEdit()
        self.min_only_input.setValidator(QDoubleValidator())
        min_only_layout.addWidget(self.min_only_input)
        
        # Max only controls
        max_only_widget = QWidget()
        max_only_layout = QVBoxLayout(max_only_widget)
        max_only_layout.addWidget(QLabel("Max Value:"))
        self.max_only_input = QLineEdit()
        self.max_only_input.setValidator(QDoubleValidator())
        max_only_layout.addWidget(self.max_only_input)
        
        # Null/Non-null don't need controls
        null_widget = QWidget()
        null_layout = QVBoxLayout(null_widget)
        null_layout.addWidget(QLabel("Show only null values"))
        
        non_null_widget = QWidget()
        non_null_layout = QVBoxLayout(non_null_widget)
        non_null_layout.addWidget(QLabel("Show only non-null values"))
        
        # Add widgets to stack
        self.filter_stack.addWidget(range_widget)
        self.filter_stack.addWidget(min_only_widget)
        self.filter_stack.addWidget(max_only_widget)
        self.filter_stack.addWidget(null_widget)
        self.filter_stack.addWidget(non_null_widget)
        
        self.content_layout.addWidget(self.filter_stack)
        
        # Initialize with range filter
        self.update_filter_controls(0)
    
    def update_filter_controls(self, index):
        self.filter_stack.setCurrentIndex(index)
    
    def get_filter_dict(self):
        base_dict = super().get_filter_dict()
        filter_dict = {
            "filterType": "QUANTITATIVE_NUMERICAL",
            "field": {
                "fieldCaption": self.field_name
            }
        }

        if hasattr(self, 'function_combo') and self.function_combo.currentText() != "":
            filter_dict["field"]["function"] = self.function_combo.currentText()
    
        
        filter_type_index = self.filter_type_combo.currentIndex()
        
        if filter_type_index == 0:  # Range
            filter_dict["quantitativeFilterType"] = "RANGE"
            if self.min_input.text():
                filter_dict["min"] = float(self.min_input.text())
            if self.max_input.text():
                filter_dict["max"] = float(self.max_input.text())
        elif filter_type_index == 1:  # Min Only
            filter_dict["quantitativeFilterType"] = "MIN"
            if self.min_only_input.text():
                filter_dict["min"] = float(self.min_only_input.text())
        elif filter_type_index == 2:  # Max Only
            filter_dict["quantitativeFilterType"] = "MAX"
            if self.max_only_input.text():
                filter_dict["max"] = float(self.max_only_input.text())
        elif filter_type_index == 3:  # Only Null
            filter_dict["quantitativeFilterType"] = "ONLY_NULL"
        elif filter_type_index == 4:  # Only Non-Null
            filter_dict["quantitativeFilterType"] = "ONLY_NON_NULL"

        return filter_dict


class DateFilterWidget(FilterWidget):
    def __init__(self, field_name, parent=None):
        super().__init__(field_name, "DATE", parent)
        
    def setup_ui(self):
        super().setup_ui()
        
        # Date filter type selection
        self.filter_type_combo = QComboBox()
        self.filter_type_combo.addItems(["Quantitative", "Relative"])
        self.filter_type_combo.currentIndexChanged.connect(self.update_filter_type)
        self.content_layout.addWidget(QLabel("Filter Type:"))
        self.content_layout.addWidget(self.filter_type_combo)
        
        # Stacked widget for different filter types
        self.filter_stack = QStackedWidget()
        
        # Quantitative date filter widget
        quant_date_widget = QWidget()
        quant_date_layout = QVBoxLayout(quant_date_widget)
        
        # Quantitative filter type selection
        self.quant_type_combo = QComboBox()
        self.quant_type_combo.addItems(["Range", "Min Date", "Max Date", "Only Null", "Only Non-Null"])
        self.quant_type_combo.currentIndexChanged.connect(self.update_quant_controls)
        quant_date_layout.addWidget(QLabel("Quantitative Type:"))
        quant_date_layout.addWidget(self.quant_type_combo)
        
        # Stacked widget for quantitative filter types
        self.quant_stack = QStackedWidget()
        
        # Range filter controls
        range_widget = QWidget()
        range_layout = QVBoxLayout(range_widget)
        
        from_layout = QHBoxLayout()
        from_layout.addWidget(QLabel("From:"))
        self.from_date = QDateEdit()
        self.from_date.setCalendarPopup(True)
        self.from_date.setDate(QDate.currentDate().addMonths(-1))
        from_layout.addWidget(self.from_date)
        range_layout.addLayout(from_layout)
        
        to_layout = QHBoxLayout()
        to_layout.addWidget(QLabel("To:"))
        self.to_date = QDateEdit()
        self.to_date.setCalendarPopup(True)
        self.to_date.setDate(QDate.currentDate())
        to_layout.addWidget(self.to_date)
        range_layout.addLayout(to_layout)
        
        # Min date only
        min_date_widget = QWidget()
        min_date_layout = QVBoxLayout(min_date_widget)
        min_date_layout.addWidget(QLabel("From Date:"))
        self.min_only_date = QDateEdit()
        self.min_only_date.setCalendarPopup(True)
        self.min_only_date.setDate(QDate.currentDate().addMonths(-1))
        min_date_layout.addWidget(self.min_only_date)
        
        # Max date only
        max_date_widget = QWidget()
        max_date_layout = QVBoxLayout(max_date_widget)
        max_date_layout.addWidget(QLabel("To Date:"))
        self.max_only_date = QDateEdit()
        self.max_only_date.setCalendarPopup(True)
        self.max_only_date.setDate(QDate.currentDate())
        max_date_layout.addWidget(self.max_only_date)
        
        # Null/Non-null don't need controls
        null_widget = QWidget()
        null_layout = QVBoxLayout(null_widget)
        null_layout.addWidget(QLabel("Show only null values"))
        
        non_null_widget = QWidget()
        non_null_layout = QVBoxLayout(non_null_widget)
        non_null_layout.addWidget(QLabel("Show only non-null values"))
        
        # Add widgets to quantitative stack
        self.quant_stack.addWidget(range_widget)
        self.quant_stack.addWidget(min_date_widget)
        self.quant_stack.addWidget(max_date_widget)
        self.quant_stack.addWidget(null_widget)
        self.quant_stack.addWidget(non_null_widget)
        
        quant_date_layout.addWidget(self.quant_stack)
        
        # Relative date filter widget
        relative_widget = QWidget()
        relative_layout = QVBoxLayout(relative_widget)
        
        # Period type selection
        self.period_type_combo = QComboBox()
        self.period_type_combo.addItems(["DAYS", "WEEKS", "MONTHS", "QUARTERS", "YEARS"])
        relative_layout.addWidget(QLabel("Period Type:"))
        relative_layout.addWidget(self.period_type_combo)
        
        # Date range type selection
        self.date_range_type_combo = QComboBox()
        self.date_range_type_combo.addItems(["LAST", "CURRENT", "NEXT", "LASTN", "NEXTN", "TODATE"])
        self.date_range_type_combo.currentIndexChanged.connect(self.update_range_n_visibility)
        relative_layout.addWidget(QLabel("Date Range Type:"))
        relative_layout.addWidget(self.date_range_type_combo)
        
        # Range N input (for LASTN and NEXTN) in a container widget
        self.range_n_widget = QWidget()
        range_n_layout = QHBoxLayout(self.range_n_widget)
        range_n_layout.setContentsMargins(0, 0, 0, 0)
        range_n_layout.addWidget(QLabel("Range N:"))
        self.range_n_input = QSpinBox()
        self.range_n_input.setMinimum(1)
        self.range_n_input.setMaximum(1000)
        self.range_n_input.setValue(30)
        range_n_layout.addWidget(self.range_n_input)
        relative_layout.addWidget(self.range_n_widget)
        
        # Add widgets to main stack
        self.filter_stack.addWidget(quant_date_widget)
        self.filter_stack.addWidget(relative_widget)
        
        self.content_layout.addWidget(self.filter_stack)
        
        # Initialize with quantitative filter
        self.update_filter_type(0)
        self.update_quant_controls(0)
        self.update_range_n_visibility(0)
    
    def update_filter_type(self, index):
        self.filter_stack.setCurrentIndex(index)
    
    def update_quant_controls(self, index):
        self.quant_stack.setCurrentIndex(index)
    
    def update_range_n_visibility(self, index):
        # Show range N input only for LASTN and NEXTN
        range_type = self.date_range_type_combo.currentText()
        needs_range_n = range_type in ["LASTN", "NEXTN"]
        self.range_n_widget.setVisible(needs_range_n)
    
    def get_filter_dict(self):
        base_dict = super().get_filter_dict()
        
        if self.filter_type_combo.currentIndex() == 0:  # Quantitative
            filter_dict = {
                "filterType": "QUANTITATIVE_DATE",
                "field": base_dict["field"]#,
                #"exclude": base_dict["exclude"]
            }
            
            quant_type_index = self.quant_type_combo.currentIndex()
            
            if quant_type_index == 0:  # Range
                filter_dict["quantitativeFilterType"] = "RANGE"
                filter_dict["minDate"] = self.from_date.date().toString(Qt.ISODate)
                filter_dict["maxDate"] = self.to_date.date().toString(Qt.ISODate)
            elif quant_type_index == 1:  # Min Date
                filter_dict["quantitativeFilterType"] = "MIN"
                filter_dict["minDate"] = self.min_only_date.date().toString(Qt.ISODate)
            elif quant_type_index == 2:  # Max Date
                filter_dict["quantitativeFilterType"] = "MAX"
                filter_dict["maxDate"] = self.max_only_date.date().toString(Qt.ISODate)
            elif quant_type_index == 3:  # Only Null
                filter_dict["quantitativeFilterType"] = "ONLY_NULL"
            elif quant_type_index == 4:  # Only Non-Null
                filter_dict["quantitativeFilterType"] = "ONLY_NON_NULL"
                
        else:  # Relative
            filter_dict = {
                "filterType": "DATE",
                "field": base_dict["field"],
                #"exclude": base_dict["exclude"],
                "periodType": self.period_type_combo.currentText(),
                "dateRangeType": self.date_range_type_combo.currentText()
            }
            
            # Add rangeN for LASTN and NEXTN
            if filter_dict["dateRangeType"] in ["LASTN", "NEXTN"]:
                filter_dict["rangeN"] = self.range_n_input.value()
                
        return filter_dict
# After your TableauApp class definition
TableauAppClass = TableauApp

# Add this at the module level (outside any class)
def run_scheduled_query_standalone(schedule_dict):
    """Standalone function to run a scheduled query"""
    try:
        print(f"Running scheduled query: {schedule_dict['name']}")
        
        # Import necessary modules
        import requests
        import datetime
        import os
        import csv
        import xml.etree.ElementTree as ET
        
        # Get auth token
        auth_token = get_auth_token()
        if not auth_token:
            return "Failed to get authentication token"
        
        # Execute the query
        headers = {
            'X-Tableau-Auth': auth_token,
            'Content-Type': 'application/json'
        }
        url = f'https://{enter_your_cluster}.online.tableau.com/api/v1/vizql-data-service/query-datasource'
        
        # Construct the query payload
        fields = [{"fieldCaption": field} for field in schedule_dict["dimensions"]]
        for field, agg in schedule_dict["measures"]:
            fields.append({
                "fieldCaption": field,
                "function": agg
            })
        
        payload = {
            "datasource": {
                "datasourceLuid": schedule_dict["datasource_luid"]
            },
            "query": {
                "fields": fields,
                "filters": schedule_dict["filters"]
            }
        }
        
        print(f"Executing query with payload: {payload}")
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            
            # Generate the output filename
            now = datetime.datetime.now()
            date_str = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%H-%M-%S")
            filename = schedule_dict["output_pattern"].format(
                name=schedule_dict["name"],
                date=date_str,
                time=time_str
            )
            
            # Ensure filename ends with .csv
            if not filename.lower().endswith('.csv'):
                filename += '.csv'
            
            # Create the full path
            output_path = os.path.join(schedule_dict["output_dir"], filename)
            
            # Export the results to CSV
            with open(output_path, 'w', newline='') as csvfile:
                # Check if data has results
                results = data.get('data', [])
                if results:
                    writer = csv.writer(csvfile)
                    # Write headers
                    headers = results[0].keys()
                    writer.writerow(headers)
                    # Write data
                    for row in results:
                        writer.writerow(row.values())
                    
                    print(f"Saved results to {output_path}")
                    return f"Query completed successfully. Results saved to {output_path}"
                else:
                    print(f"Query returned no results")
                    return "Query completed but returned no results"
        else:
            error_msg = f"Query failed with status {response.status_code}: {response.text}"
            print(error_msg)
            return error_msg
            
    except Exception as e:
        error_msg = f"Error running scheduled query '{schedule_dict.get('name', 'unknown')}': {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        return error_msg

def get_auth_token():
    """Get a Tableau auth token"""
    url = 'https://{enter_your_cluster}.online.tableau.com/api/3.25/auth/signin'
    payload = {
        "credentials": {
            "personalAccessTokenName": "{enter_your_token_name}",
            "personalAccessTokenSecret": "{enter_your_token_secret}",
            "site": {
                "contentUrl": "{enter_your_site_name}"
            }
        }
    }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.text)
        credentials = root.find('.//{http://tableau.com/api}credentials')
        return credentials.attrib['token']
    else:
        raise Exception(f"Authentication failed: {response.status_code} - {response.text}")


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

def main():
    app = QApplication(sys.argv)
    
    # Splash screen code
    if not hasattr(sys, '_MEIPASS'):  # Not running from PyInstaller
        splash_image_path = "C:/Users/DarenCullimore/Pictures/TableauQueryMeme.jpg" #Replace with your file path
    else:
        splash_image_path = resource_path("TableauQueryMeme.jpg")
    
    print(f"Looking for splash image at: {splash_image_path}")
    print(f"File exists: {os.path.exists(splash_image_path)}")
    
    splash_pixmap = QPixmap(splash_image_path)
    
    if splash_pixmap.isNull():
        # Fallback to generated splash screen
        splash_pixmap = QPixmap(400, 200)
        splash_pixmap.fill(QColor("#2D72D7"))
        painter = QPainter(splash_pixmap)
        painter.setPen(Qt.white)
        painter.setFont(QFont('Arial', 14, QFont.Bold))
        painter.drawText(splash_pixmap.rect(), Qt.AlignCenter, "Tableau Query Tool\n\nLoading...")
        painter.end()
    
    splash = QSplashScreen(splash_pixmap)
    splash.show()
    app.processEvents()
    
    # Define initialization function
    def initialize_app():
        try:
            # Store the main window as a global variable to prevent garbage collection
            global main_window
            main_window = TableauApp()
            main_window.show()
            splash.finish(main_window)
        except Exception as e:
            print(f"Error initializing application: {e}")
            import traceback
            traceback.print_exc()
            # Show error message box
            from PyQt5.QtWidgets import QMessageBox
            error_box = QMessageBox()
            error_box.setIcon(QMessageBox.Critical)
            error_box.setWindowTitle("Application Error")
            error_box.setText("An error occurred while starting the application.")
            error_box.setDetailedText(f"Error: {str(e)}\n\n{traceback.format_exc()}")
            error_box.exec_()
            sys.exit(1)
    
    # Delay initialization
    QTimer.singleShot(1000, initialize_app)
    
    # This will keep the application running
    return app.exec_()

if __name__ == "__main__":
    sys.exit(main())