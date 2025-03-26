import sys
import polars as pl
import os
from PyQt5.QtWidgets import QApplication, QLabel, QVBoxLayout, QHBoxLayout, QWidget, QStackedLayout, QComboBox, QLineEdit, QCompleter, QListWidget, QAbstractItemView, QSpinBox, QPushButton, QMessageBox, QScrollArea, QSpacerItem, QSizePolicy
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtCore import  Qt, QUrl

from custom_database import searchingAPI

# function to generate URL from filepath
def generate_link_html(file_path):
    fn = os.path.basename(file_path)
    return f'<a href="file://{file_path}">{fn}</a>'


# Main Comps Searcher Widget 
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.api  = searchingAPI("/Database")
        # self.api = searchingAPI("/Users/rebeccakrall/Desktop/Database4")

        # initialize all search parameters
        self.methods_list = None
        self.methods_str = None
        self.cmpd_list = None
        self.client = None
        self.sex = None
        self.species = None
        self.strain = None
        (self.minyear, self.maxyear) = self.api.get_possible_years()
        self.lower_year = self.minyear
        self.upper_year = self.maxyear
        self.filepaths = []
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Comps Finder')
        self.stacked_layout = QStackedLayout()
        self._createSearchPage()
        self._createFileDisplay()
        
        main_layout = QVBoxLayout()
        main_layout.addLayout(self.stacked_layout)
        self.setLayout(main_layout)

    # initial search page construction
    def _createSearchPage(self):
        self.searchPage = QWidget()
        search_layout = QVBoxLayout(self.searchPage)

        # Heading label
        info = QLabel("Search for Proposals/Reports Matching Criteria")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        search_layout.addWidget(info)

        # List to choose from assays
        # method_label = QLabel('Choose from standard assays')
        # self.method_combo = QComboBox()
        # all_methods = self.api.get_possible_methods()
        # all_methods.insert(0, "")
        # self.method_combo.addItems(all_methods)
        # self.method_combo.setEditable(True)
        # completer = QCompleter(all_methods, parent=self.method_combo)

        # self.method_combo.setCompleter(completer)
        # self.method_combo.activated.connect(self.method_choice)
        # search_layout.addWidget(method_label)
        # search_layout.addWidget(self.method_combo)
        
        method_label = QLabel('Choose from standard assays')
        self.method_combo = QListWidget()
        all_methods = self.api.get_possible_methods()
        self.method_combo.addItems(all_methods)
        self.method_combo.itemSelectionChanged.connect(self.method_choice)
        self.method_combo.setSelectionMode(QAbstractItemView.MultiSelection)
        search_layout.addWidget(method_label)
        search_layout.addWidget(self.method_combo)

    
        # # Choose from common compounds
        cmpd_label = QLabel('Choose from common compounds')
        self.cmpds_combo = QListWidget()
        all_cmpds = self.api.get_possible_compounds()
        self.cmpds_combo.addItems(all_cmpds)
        self.cmpds_combo.itemSelectionChanged.connect(self.cmpds_choice)
        self.cmpds_combo.setSelectionMode(QAbstractItemView.MultiSelection)
        search_layout.addWidget(cmpd_label)
        search_layout.addWidget(self.cmpds_combo)

        # choose from possible clients
        client_label = QLabel('Choose client')
        self.client_combo = QComboBox()
        all_clients = self.api.get_possible_clients()
        all_clients.insert(0, "")
        self.client_combo.addItems(all_clients)
        self.client_combo.setEditable(True)
        completer = QCompleter(all_clients, parent=self.client_combo)
        
        self.client_combo.setCompleter(completer)
        self.client_combo.activated.connect(self.client_choice)
        search_layout.addWidget(client_label)
        search_layout.addWidget(self.client_combo)

        # Choose from all years with documents available
        hbox = QHBoxLayout()
        label = QLabel('Select Year Range')
        hbox.addWidget(label)
        self.lower = QSpinBox()
        self.lower.setRange(self.minyear, self.maxyear)
        self.lower.setValue(self.minyear)
        self.lower.valueChanged.connect(self.update_lower)
        hbox.addWidget(self.lower)
        inbetween = QLabel('to')
        hbox.addWidget(inbetween)
        self.upper = QSpinBox()
        self.upper.setRange(self.minyear, self.maxyear)
        self.upper.setValue(self.maxyear)
        self.upper.valueChanged.connect(self.update_upper)
        hbox.addWidget(self.upper)
        search_layout.addLayout(hbox)

        # Choose species, strain, sex, source
        ahbox = QHBoxLayout()
        splabel = QLabel('Species')
        ahbox.addWidget(splabel)
        self.species_combo = QComboBox()
        self.species_combo.addItems(["",'mouse','rat','rat and mouse'])
        self.species_combo.activated.connect(self.species_choice)
        ahbox.addWidget(self.species_combo)

        sxlabel = QLabel('Sex')
        ahbox.addWidget(sxlabel)
        self.sex_combo = QComboBox()
        self.sex_combo.addItems(["",'males','females','both'])
        self.sex_combo.activated.connect(self.sex_choice)
        ahbox.addWidget(self.sex_combo)

        stlabel = QLabel('Strain')
        ahbox.addWidget(stlabel)
        self.strain_combo = QComboBox()
        strains = self.api.get_possible_strains()
        strains.insert(0, "")
        self.strain_combo.addItems(strains)
        self.strain_combo.activated.connect(self.strain_choice)
        ahbox.addWidget(self.strain_combo)

        search_layout.addLayout(ahbox)

        # Search Button
        enter_button = QPushButton("Search")
        enter_button.clicked.connect(self.search_docs)
        search_layout.addWidget(enter_button, alignment=Qt.AlignRight)
        self.stacked_layout.addWidget(self.searchPage)

    def _createFileDisplay(self):
        
        self.fileStack = QWidget()
        self.fileStackLayout = QVBoxLayout(self.fileStack)
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        self.fileStackLayout.addWidget(back_button, alignment=Qt.AlignRight)

        self.comp_count = QLabel(str(len(self.filepaths)) + " documents found")
        self.fileStackLayout.addWidget(self.comp_count)
        
        self.fileDisplay = QWidget()
        self.scroll = QScrollArea()
        self.display_layout = QVBoxLayout(self.fileDisplay)
        self.link_labels = []
        self.scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.scroll.setWidgetResizable(True)
        self.scroll.setWidget(self.fileDisplay)
        self.fileStackLayout.addWidget(self.scroll)
        
        
        self.stacked_layout.addWidget(self.fileStack)
        
    def add_link(self, filepath, description):

        self.link_labels.append(QLabel())
        # fl = generate_link_html(filepath)
        plain_text = f'{description}:&emsp;'  # Add an em space for indentation
        fl = f'<a href="file://{filepath}">{os.path.basename(filepath)}</a>'
        self.link_labels[-1].setText(f'{plain_text}{fl}')
        # self.link_labels[-1].setText(fl)
        self.link_labels[-1].setOpenExternalLinks(True)
        self.link_labels[-1].setAlignment(Qt.AlignLeft)
        self.link_labels[-1].setStyleSheet("padding-left: 20px;") 
        self.display_layout.addWidget(self.link_labels[-1])
        self.link_labels[-1].linkActivated.connect(self.open_document)

    def add_text(self, text, title = False):
        txt = QLabel()
        txt.setText(text)
        txt.setAlignment(Qt.AlignLeft)
        if title:
            txt.setStyleSheet("text-decoration: underline; font-weight: bold")
        self.display_layout.addWidget(txt)

    
    def open_document(self, url):
        QDesktopServices.openUrl(QUrl(url))

    def method_choice(self):
        self.methods_list = [item.text() for item in self.method_combo.selectedItems()]
        if len(self.methods_list) == 0:
            self.methods_list = None

    def heading_choice(self):
        self.methods_str = self.heading_edit.text()
        if self.methods_str == '':
            self.methods_str = None
        
    def cmpds_choice(self):
        self.cmpd_list = [item.text() for item in self.cmpds_combo.selectedItems()]
        if len(self.cmpd_list) == 0:
            self.cmpd_list = None
    
    def client_choice(self):
        self.client = self.client_combo.currentText()
        if self.client == "":
            self.client = None

    def species_choice(self):
        self.species = self.species_combo.currentText()
        if self.species == "":
            self.species = None

    def sex_choice(self):
        self.sex = self.sex_combo.currentText()
        if self.sex == "":
            self.sex = None

    def strain_choice(self):
        self.strain = self.strain_combo.currentText()
        if self.strain == "":
            self.strain = None

    def update_lower(self):
        self.lower_year = self.lower.value()
        self.upper.setRange(self.lower.value(), self.maxyear)
    
    def update_upper(self):
        self.upper_year = self.upper.value()
        self.lower.setRange(self.minyear, self.upper.value())
    
    def search_docs(self):    
        
        self.api.create_filtered()
        if self.methods_list is not None:
            for method in self.methods_list:
                self.api.filter_by_method(method)
                
                if self.api.check_filter_empty():
                    QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                    return
              

        if self.cmpd_list is not None:
           for c in self.cmpd_list:
                self.api.filter_by_compound(c)
                
                if self.api.check_filter_empty():
                    QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                    return
        
        
        if self.client is not None:
            self.api.filter_by_client(self.client)
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
       
        if self.sex is not None:
            self.api.filter_by_sex(self.sex)
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
        
        if self.strain is not None:
            self.api.filter_by_strain(self.strain)
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
        
        if self.species is not None:
            self.api.filter_by_species(self.species)
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
            

        self.api.filter_by_date((self.lower_year, self.upper_year))
        if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return

        
            # self.found_docs = fin

        self.filepaths = self.api.get_matching_docs()
        
        for row in self.filepaths.iter_rows():
            # row[0] = study id
            self.add_text(row[0], title = True)
            if row[1] is not None:
                # self.add_text('Latest Proposal')
                self.add_link(row[1], 'Latest Proposal')
            if row[2] is not None:
                # self.add_text('Latest Report')
                self.add_link(row[2], 'Latest Report')
            # row[1] = proposal
            # row[2] = report

        # for row in self.filepaths:
        #     self.add_link(row)
    
        self.comp_count.setText(str(len(self.filepaths)) + " studies found:")
        self.stacked_layout.setCurrentIndex(1)
        return 
        
    def last_page(self):
        self.clearLayout(self.display_layout)
        self.methods_list = None
        self.methods_str = None
        # self.heading_edit.clear()
        self.cmpd_list = None
        self.client = None
        self.lower_year = self.minyear
        self.upper_year = self.maxyear
        # self.found_docs = pl.read_excel("all_filename_detail_050442.xlsx")
        self.upper.setValue(self.maxyear)
        self.lower.setValue(self.minyear)
        self.stacked_layout.setCurrentIndex(0)
        self.api.create_filtered()
        # self.doc_ref = pl.read_excel("all_filename_detail_050442.xlsx")
     
    def clearLayout(self, layout):
        if layout is not None:
            while layout.count():
                child = layout.takeAt(0)
                if child.widget() is not None:
                    child.widget().deleteLater()
                elif child.layout() is not None:
                    self.clearLayout(child.layout())

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())