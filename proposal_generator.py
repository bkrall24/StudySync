import sys
import polars as pl
from PyQt5.QtWidgets import QApplication, QLabel, QVBoxLayout, QHBoxLayout, QWidget, QStackedLayout, QComboBox, QLineEdit, QListWidget, QAbstractItemView, QSpinBox, QPushButton, QMessageBox, QDateEdit, QCheckBox, QTableWidget,QTableWidgetItem, QFileDialog, QDialog, QTextEdit, QScrollArea
from PyQt5.QtCore import  Qt, QDate
from functools import partial  

from custom_database import searchingAPI, referenceAPI
from proposal_filling import *

class MainWindow(QWidget):

    def __init__(self):
        super().__init__()

        self.api = searchingAPI("/Volumes/Company/Becca/Study Database/Database")
        self.ref = referenceAPI("/Volumes/Company/Becca/Study Database/Reference")
        # initialize all parameters
        self.page_index = 0 
        self.title = None
        self.date = QDate.currentDate()
        self.client = None
        self.study_id = None
        self.model = None
        self.species = None
        self.strain = None
        self.source = None
        self.age = None
        self.sex = None
        self.rand = False
        self.random_txt = 'The study will not be randomized'
        self.blind = False
        self.blind_txt = 'The study will not be blinded'
        self.acc = "Not less than five days"
        self.light_cycle = "Mice will be housed on a 12 hr light/dark cycle (lights on 7:00 AM) "
        self.mice_per_cage = "No more than 4 mice per cage depending on size"
        self.diet_water = "Standard rodent chow and water ad libitum"
        self.route = None
        self.dose_vol = None
        self.formulation = f'To be provided by {self.client} or Melior'
        self.dose_level = None
        self.dose_freq = None
        self.duration = None
        self.pretreatment = None
        self.number_groups = None
        self.animal_groups = None
        self.number_animals = None
        self.table_cols = ["Group #", "Treatment", "Group Size", "Days of Dosing", "Dose", "Route", "Evaluation/Endpoints"]
        self.pm = None
        self.pc = None
        self.cms = None
        self.save_path = None
        self.methods = {}
        self.row_num = 3
        self.tbl_contents = []
        self.added = False
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Proposal Generator')
        self.stacked_layout = QStackedLayout()
        self._createStartPage()
        self._createAnimalDescription()
        self._createHousingAndFeeding()
        self._createDesign()
        self._createDesignTable()
        self._createPeopleChoice()
        self._createMethods()
        self._createMethodDescriptions()
        self._createFinalPage()
        
        main_layout = QVBoxLayout()
        main_layout.addLayout(self.stacked_layout)
        self.setLayout(main_layout)

    # Initial Page - basic proposal details 
    def _createStartPage(self):
        self.startPage = QWidget()
        self.start_layout = QVBoxLayout(self.startPage)
        # start_layout.setAlignment(Qt.AlignTop)

        # add header
        info = QLabel("Provide Information for Proposal:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        self.start_layout.addWidget(info, alignment=Qt.AlignTop)

        
        # add title
        title_box = QHBoxLayout()
        title_label = QLabel('Title:')
        title_box.addWidget(title_label)
        self.title_edit = QLineEdit()
        self.title_edit.setPlaceholderText("Enter proposal title here")
        self.title_edit.textChanged.connect(lambda text: self.update_variable(text, 'title'))
        title_box.addWidget(self.title_edit)
        title_box.setAlignment(Qt.AlignTop)
        self.start_layout.addLayout(title_box)

        # add client
        self.possible_clients = self.api.get_possible_clients()
        self.possible_clients.insert(0, None)
        client_box = QHBoxLayout()
        client_label = QLabel('Client:')
        client_box.addWidget(client_label)
        self.client_widget = QComboBox(self)
        self.client_widget.setEditable(True)
        self.client_widget.addItems(self.possible_clients)
        # self.client_widget.editTextChanged.connect(self.client_added)
        self.client_widget.lineEdit().returnPressed.connect(self.client_added)

        self.client_widget.activated.connect(self.client_clicked)
        client_box.addWidget(self.client_widget)
        client_box.setAlignment(Qt.AlignTop)
        self.start_layout.addLayout(client_box)
        
        # add date
        date_box = QHBoxLayout()
        date_label = QLabel('Date:')
        date_box.addWidget(date_label)
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)  # Show a popup calendar when clicking the widget
        self.date_edit.setDate(self.date)  # Set the initial date to today
        self.date_edit.dateChanged.connect(self.on_date_changed)
        date_box.addWidget(self.date_edit)
        date_box.setAlignment(Qt.AlignTop)
        self.start_layout.addLayout(date_box)
        
        # add primary method
        model_box = QHBoxLayout()
        model_label = QLabel('Primary Model:')
        model_box.addWidget(model_label)
        self.possible_models = self.ref.get_possible_models()
        self.possible_models.insert(0, None)

        self.model_widget = QComboBox(self)
        self.model_widget.setEditable(True)
        self.model_widget.addItems(self.possible_models)
        # self.client_widget.editTextChanged.connect(self.client_added)
        self.model_widget.lineEdit().returnPressed.connect(self.model_added)
        self.model_widget.activated.connect(self.model_clicked)



        # self.model_widget = QComboBox(self)
        # self.model_widget.setEditable(True)
        # self.model_widget.addItems(self.possible_models)
        # self.model_widget.activated.connect(self.model_clicked)
        model_box.addWidget(self.model_widget)
        model_box.setAlignment(Qt.AlignTop)
        self.start_layout.addLayout(model_box)

        # add study id
        id_box = QHBoxLayout()
        id_label = QLabel('Study ID:')
        id_box.addWidget(id_label)
        self.id_edit = QLineEdit()
        self.id_edit.setText(self.study_id)
        self.id_edit.textChanged.connect(lambda text: self.update_variable(text, 'study_id'))
        id_box.addWidget(self.id_edit)
        id_box.setAlignment(Qt.AlignTop)
        self.start_layout.addLayout(id_box)

        # add description
        desc_box = QHBoxLayout()
        desc_label = QLabel('Description (for header):')
        desc_box.addWidget(desc_label)
        self.desc_edit = QLineEdit()
        self.get_description()
        self.desc_edit.setText(self.desc)
        self.desc_edit.textChanged.connect(lambda text: self.update_variable(text, 'desc'))
        desc_box.addWidget(self.desc_edit)
        desc_box.setAlignment(Qt.AlignTop)
        self.start_layout.addLayout(desc_box)

        # Enter Button
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        self.start_layout.addWidget(enter_button, alignment=Qt.AlignRight)



        self.stacked_layout.addWidget(self.startPage)

    # Fill out animal description
    def _createAnimalDescription(self):
        
        # create page
        self.adPage = QWidget()
        ad_layout = QVBoxLayout(self.adPage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Animal Description:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        
        # add back button
        # back_button = QPushButton("Back")
        # back_button.clicked.connect(self.last_page)
        # head_box.addWidget(back_button, alignment=Qt.AlignRight)
        ad_layout.addLayout(head_box)
        
        # species
        self.possible_species = [None, 'Mouse', 'Rat', 'Rat and Mouse']
        species_box = QHBoxLayout()
        species_label = QLabel('Species: ')
        species_box.addWidget(species_label)
        self.species_widget = QComboBox(self)
        self.species_widget.addItems(self.possible_species)
        self.species_widget.activated.connect(self.species_clicked)
        species_box.addWidget(self.species_widget)
        ad_layout.addLayout(species_box)

        # strain
        self.possible_strains = self.ref.get_possible_strains()
        self.possible_strains.insert(0, None)
        strain_box = QHBoxLayout()
        strain_label = QLabel('Strain:')
        strain_box.addWidget(strain_label)
        self.strain_widget = QComboBox(self)
        self.strain_widget.setEditable(True)
        self.strain_widget.addItems(self.possible_strains)
        self.strain_widget.activated.connect(self.strain_clicked)
        self.strain_widget.editTextChanged.connect(lambda text: self.update_variable(text, 'strain'))
        strain_box.addWidget(self.strain_widget)
        ad_layout.addLayout(strain_box)
        
        # source
        source_box = QHBoxLayout()
        source_label = QLabel('Source:')
        source_box.addWidget(source_label)
        self.source_edit = QLineEdit()
        self.source_edit.setPlaceholderText("Enter animal source here")
        self.source_edit.textChanged.connect(lambda text: self.update_variable(text, 'source'))
        source_box.addWidget(self.source_edit)
        ad_layout.addLayout(source_box)

        # age
        age_box = QHBoxLayout()
        age_label = QLabel('Age:')
        age_box.addWidget(age_label)
        self.age_edit = QLineEdit()
        self.age_edit.setPlaceholderText("Enter animal age here")
        self.age_edit.textChanged.connect(lambda text: self.update_variable(text, 'age'))
        age_box.addWidget(self.age_edit)
        ad_layout.addLayout(age_box)

        # sex
        self.possible_sexes = [None, 'Males', 'Females', 'Both']
        sex_box = QHBoxLayout()
        sex_label = QLabel('Sex: ')
        sex_box.addWidget(sex_label)
        self.sex_widget = QComboBox(self)
        self.sex_widget.addItems(self.possible_sexes)
        self.sex_widget.activated.connect(self.sex_clicked)
        sex_box.addWidget(self.sex_widget)
        ad_layout.addLayout(sex_box)

        # randomization
        random_box = QHBoxLayout()
        random_label = QLabel('Randomization: ')
        random_box.addWidget(random_label)
        self.random_checkbox = QCheckBox("Randomized")
        self.random_checkbox.stateChanged.connect(self.rand_state_changed)
        random_box.addWidget(self.random_checkbox)
        self.random_edit = QLineEdit(self)
        self.random_edit.setText(self.random_txt)
        self.random_edit.textChanged.connect(lambda text: self.update_variable(text, 'random_txt'))
        random_box.addWidget(self.random_edit)
        ad_layout.addLayout(random_box)

        # blinding
        blind_box = QHBoxLayout()
        blind_label = QLabel('Blinding: ')
        blind_box.addWidget(blind_label)
        self.blinded_checkbox = QCheckBox("Blinded")
        self.blinded_checkbox.stateChanged.connect(self.blind_state_changed)
        blind_box.addWidget(self.blinded_checkbox)
        self.blind_edit = QLineEdit(self)
        self.blind_edit.setText(self.blind_txt)
        self.blind_edit.textChanged.connect(lambda text: self.update_variable(text, 'blind_txt'))
        blind_box.addWidget(self.blind_edit)
        ad_layout.addLayout(blind_box)

        # enter_button = QPushButton("Next")
        # enter_button.clicked.connect(self.next_page)
        # ad_layout.addWidget(enter_button, alignment=Qt.AlignRight)

        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        ad_layout.addLayout(button_box)

        self.stacked_layout.addWidget(self.adPage)
    
    # Fill out housing and feeding
    def _createHousingAndFeeding(self):
        # create page
        self.hfPage = QWidget()
        hf_layout = QVBoxLayout(self.hfPage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Housing and Feeding:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        
        # add back button
        # back_button = QPushButton("Back")
        # back_button.clicked.connect(self.last_page)
        # head_box.addWidget(back_button, alignment=Qt.AlignRight)
        hf_layout.addLayout(head_box)


        
        acc_label = QLabel('Acclimation: ')
        hf_layout.addWidget(acc_label)
        self.acc_edit = QLineEdit(self)
        self.acc_edit.setText(self.acc)
        self.acc_edit.textChanged.connect(lambda text: self.update_variable(text, 'acc'))
        hf_layout.addWidget(self.acc_edit)

        housing_label = QLabel('Housing: ')
        hf_layout.addWidget(housing_label)

        self.light_edit= QLineEdit(self)
        self.light_edit.setText(self.light_cycle)
        self.light_edit.textChanged.connect(lambda text: self.update_variable(text, 'light_cycle'))
        hf_layout.addWidget(self.light_edit)

        self.mpc_edit = QLineEdit(self)
        self.mpc_edit.setText(self.mice_per_cage)
        self.mpc_edit.textChanged.connect(lambda text: self.update_variable(text, 'mice_per_cage'))
        hf_layout.addWidget(self.mpc_edit)

        diet_label = QLabel('Diet & Water: ')
        hf_layout.addWidget(diet_label)
        self.diet_edit = QLineEdit(self)
        self.diet_edit.setText(self.diet_water)
        self.diet_edit.textChanged.connect(lambda text: self.update_variable(text, 'diet_water'))
        hf_layout.addWidget(self.diet_edit)


        # enter_button = QPushButton("Next")
        # enter_button.clicked.connect(self.next_page)
        # hf_layout.addWidget(enter_button, alignment=Qt.AlignRight)

        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        hf_layout.addLayout(button_box)

        self.stacked_layout.addWidget(self.hfPage)

    # Fill out design 
    def _createDesign(self):

        # tb implemented - option to set it as bullet point or not

        # create page
        self.designPage = QWidget()
        design_layout = QVBoxLayout(self.designPage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Design:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        design_layout.addLayout(head_box)

        # add route of admin
        route_box = QHBoxLayout()
        route_box.addWidget(QLabel('Route(s) of administration: '))
        self.route_edit = QLineEdit(self)
        self.route_edit.setText(self.route)
        self.route_edit.textChanged.connect(lambda text: self.update_variable(text, 'route'))
        route_box.addWidget(self.route_edit)
        design_layout.addLayout(route_box)

        # add dose volumes
        vol_box = QHBoxLayout()
        vol_box.addWidget(QLabel('Dose Volume(s): '))
        self.vol_edit = QLineEdit(self)
        self.vol_edit.setText(self.dose_vol)
        self.vol_edit.textChanged.connect(lambda text: self.update_variable(text, 'dose_vol'))
        vol_box.addWidget(self.vol_edit)
        design_layout.addLayout(vol_box)

        # formulations
        form_box = QHBoxLayout()
        form_box.addWidget(QLabel('Formulations'))
        self.form_edit = QLineEdit(self)
        self.form_edit.setText(self.formulation)
        self.form_edit.textChanged.connect(lambda text: self.update_variable(text, 'formulation'))
        form_box.addWidget(self.form_edit)
        design_layout.addLayout(form_box)

        # dose levels
        level_box = QHBoxLayout()
        level_box.addWidget(QLabel('Dose Level(s): '))
        self.level_edit = QLineEdit(self)
        self.level_edit.setText(self.dose_level)
        self.level_edit.textChanged.connect(lambda text: self.update_variable(text,'dose_level'))
        level_box.addWidget(self.level_edit)
        design_layout.addLayout(level_box)

        # dose frequency
        freq_box = QHBoxLayout()
        freq_box.addWidget(QLabel('Dose Frequency:'))
        self.freq_edit = QLineEdit(self)
        self.freq_edit.setText(self.dose_freq)
        self.freq_edit.textChanged.connect(lambda text: self.update_variable(text, 'dose_freq'))
        freq_box.addWidget(self.freq_edit)
        design_layout.addLayout(freq_box)

        # study duration
        dur_box = QHBoxLayout()
        dur_box.addWidget(QLabel('Study duration: '))
        self.dur_edit = QLineEdit(self)
        self.dur_edit.setText(self.duration)
        self.dur_edit.textChanged.connect(lambda text: self.update_variable(text, 'duration'))
        dur_box.addWidget(self.dur_edit)
        design_layout.addLayout(dur_box)

        # pretreatment time
        pre_box = QHBoxLayout()
        pre_box.addWidget(QLabel('Pretreatment time: '))
        self.pre_edit = QLineEdit(self)
        self.pre_edit.setText(self.pretreatment)
        self.pre_edit.textChanged.connect(lambda text: self.update_variable(text, 'pretreatment'))
        pre_box.addWidget(self.pre_edit)
        design_layout.addLayout(pre_box)

        # number of groups
        group_box = QHBoxLayout()
        group_box.addWidget(QLabel('Number of Groups:'))
        self.group_edit = QLineEdit(self)
        self.group_edit.setText(self.number_groups)
        self.group_edit.textChanged.connect(lambda text: self.update_numbers(text, 'number_groups'))
        group_box.addWidget(self.group_edit)
        design_layout.addLayout(group_box)

        # number of animals per group
        per_box = QHBoxLayout()
        per_box.addWidget(QLabel('Number of animals per group: '))
        self.per_edit = QLineEdit(self)
        self.per_edit.setText(self.animal_groups)
        self.per_edit.textChanged.connect(lambda text: self.update_numbers(text, 'animal_groups'))
        per_box.addWidget(self.per_edit)
        design_layout.addLayout(per_box)

        # total number of animals
        
        total_box = QHBoxLayout()
        total_box.addWidget(QLabel('Total number of animals: '))
        self.total_edit = QLineEdit(self)
        self.total_edit.setText(self.number_animals)
        self.total_edit.textChanged.connect(lambda text: self.update_numbers(text, 'number_animals'))
        total_box.addWidget(self.total_edit)
        design_layout.addLayout(total_box)


        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        design_layout.addLayout(button_box)

        self.stacked_layout.addWidget(self.designPage)

    def _createDesignTable(self):
        
        # create page
        self.tablePage = QWidget()
        table_layout = QVBoxLayout(self.tablePage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Design Table:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        table_layout.addLayout(head_box)
        
        standard_columns = ["Group #", "Treatment", "Group Size", "Days of Dosing", "Dose", "Route", "Evaluation/Endpoints"]
        column_box = QHBoxLayout()
        self.col_checks = []
        for ind, col in enumerate(standard_columns):
            self.col_checks.append(QCheckBox(col))
            self.col_checks[ind].setChecked(True)
            self.col_checks[ind].stateChanged.connect(lambda state: self.col_choice(state))
            column_box.addWidget(self.col_checks[ind])
        table_layout.addLayout(column_box)


        label_box = QHBoxLayout()
        label_box.addWidget(QLabel('Choose number of rows: '))
        self.row_count = QSpinBox()
        self.row_count.setRange(1, 100)
      
        self.row_count.setValue(self.row_num)


        self.row_count.valueChanged.connect(self.update_row_count)
        label_box.addWidget(self.row_count)
        table_layout.addLayout(label_box)

        self.table = QTableWidget(self.row_num, len(self.table_cols))
        self.table.setHorizontalHeaderLabels(self.table_cols)
        self.table.setVerticalHeaderLabels([str(i+1) for i in range(self.row_num)])

        # # Populate cells with initial data and make them editable
        for row in range(self.row_num):
            self.tbl_contents.append([])
            for col in range(len(self.table_cols)):
                if self.table_cols[col] == "Group #":
                    self.tbl_contents[-1].append(str(row+1))
                elif (self.table_cols[col] == "Group Size") and (self.animal_groups is not None):
                    self.tbl_contents[-1].append(str(self.animal_groups))
                else:
                    self.tbl_contents[row].append('')
        
        self.update_table()
        # print(f'Rows: {len(self.tbl_contents)}, Columns: {len(self.tbl_contents[0])}')
        # Set layout
        self.table.cellChanged.connect(self.cell_changed)
        table_layout.addWidget(self.table)

        # merge_box = QHBoxLayout()
        # merge_button = QPushButton("Merge Selected Cells")
        # merge_button.clicked.connect(self.merge_selected_cells)
        # merge_box.addWidget(merge_button, alignment=Qt.AlignRight)
        # unmerge_button = QPushButton("Unmerge Selected Cells")
        # unmerge_button.clicked.connect(self.unmerge_selected_cells)
        # merge_box.addWidget(unmerge_button, alignment=Qt.AlignRight)
        # table_layout.addLayout(merge_box)



        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        table_layout.addLayout(button_box)

        self.stacked_layout.addWidget(self.tablePage)
        
    # Choose or add methods
    def _createMethods(self):
        
        # create page
        self.methodPage = QWidget()
        method_layout = QVBoxLayout(self.methodPage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Methods:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        method_layout.addLayout(head_box)

        choice_box = QVBoxLayout()
        choice_box.addWidget(QLabel("Choose methods for proposal"))
        self.method_widget = QListWidget()
        self.method_widget.addItems(self.possible_models)
        self.method_widget.itemSelectionChanged.connect(self.method_choice)
        self.method_widget.setSelectionMode(QAbstractItemView.MultiSelection)
        self.method_widget.item(0).setSelected(False)
        self.method_widget.setMaximumHeight(150)  # Set a max height
        self.method_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        choice_box.addWidget(self.method_widget)
        method_layout.addLayout(choice_box)

        add_box = QVBoxLayout()
        add_box.addWidget(QLabel('Add additional methods to add as headers'))
        self.method_add = QLineEdit()
        # self.method_add.textChanged.connect(self.method_update)
        method_button = QPushButton("add")
        method_button.clicked.connect(self.method_update)
        add_box.addWidget(self.method_add)
        add_box.addWidget(method_button, alignment = Qt.AlignRight)
        
        method_layout.addLayout(add_box)

        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        method_layout.addLayout(button_box)

        self.stacked_layout.addWidget(self.methodPage)

    def _createMethodDescriptions(self):
        self.methodDesc = QWidget()
        self.desc_layout = QVBoxLayout(self.methodDesc)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Method Descriptions:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        self.desc_layout.addLayout(head_box)

        # content
        self.text_edits = {}
        self.description_box = QVBoxLayout()
        self.desc_layout.addLayout(self.description_box)

        # self.setLayout(desc_layout)

        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        self.desc_layout.addLayout(button_box)

        self.stacked_layout.addWidget(self.methodDesc)
    
    # Choose or add people
    def _createPeopleChoice(self):
        # create page
        self.peoplePage = QWidget()
        people_layout = QVBoxLayout(self.peoplePage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Key Personnel:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        people_layout.addLayout(head_box)

        
        self.possible_employees = self.ref.get_possible_employees()
        self.possible_employees.insert(0, None)

        # Project Manager
        pm_box = QHBoxLayout()
        pm_label = QLabel('Project Manager:')
        pm_box.addWidget(pm_label)
        self.pm_widget = QComboBox(self)
        self.pm_widget.setEditable(True)
        self.pm_widget.addItems(self.possible_employees)
        self.pm_widget.activated.connect(self.pm_clicked)
        self.pm_widget.editTextChanged.connect(lambda text: self.update_variable(text, 'pm'))
        pm_box.addWidget(self.pm_widget)
        people_layout.addLayout(pm_box)

        # Project Coordinator
        pc_box = QHBoxLayout()
        pc_label = QLabel('Project Coordinator:')
        pc_box.addWidget(pc_label)
        self.pc_widget = QComboBox(self)
        self.pc_widget.setEditable(True)
        self.pc_widget.addItems(self.possible_employees)
        self.pc_widget.activated.connect(self.pc_clicked)
        self.pc_widget.editTextChanged.connect(lambda text: self.update_variable(text, 'pc'))
        pc_box.addWidget(self.pc_widget)
        people_layout.addLayout(pc_box)

        # Client Managment Specialist
        cms_box = QHBoxLayout()
        cms_label = QLabel('Client Management Specialist:')
        cms_box.addWidget(cms_label)
        self.cms_widget = QComboBox(self)
        self.cms_widget.setEditable(True)
        self.cms_widget.addItems(self.possible_employees)
        self.cms_widget.activated.connect(self.cms_clicked)
        self.cms_widget.editTextChanged.connect(lambda text: self.update_variable(text, 'cms'))
        cms_box.addWidget(self.cms_widget)
        people_layout.addLayout(cms_box)
        
        # add buttons
        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        enter_button = QPushButton("Next")
        enter_button.clicked.connect(self.next_page)
        button_box.addWidget(enter_button, alignment=Qt.AlignRight)
        button_box.setAlignment(Qt.AlignBottom)
        people_layout.addLayout(button_box)


        self.stacked_layout.addWidget(self.peoplePage)

    # Choose save location and create document
    def _createFinalPage(self):
        self.finalPage = QWidget()
        self.final_layout = QVBoxLayout(self.finalPage)

        # add header
        head_box = QHBoxLayout()
        info = QLabel("Generate Proposal:")
        font = info.font()
        font.setPointSize(16)
        font.setBold(True)
        info.setFont(font)
        head_box.addWidget(info)
        head_box.setAlignment(Qt.AlignTop)
        self.final_layout.addLayout(head_box)
        # choose a location and filename to save

        save_button = QPushButton("Save File")
        save_button.clicked.connect(self.save_file)
        self.final_layout.addWidget(save_button, alignment = Qt.AlignTop)


        # add buttons
        button_box = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        button_box.addWidget(back_button, alignment=Qt.AlignLeft)
        self.final_layout.addLayout(button_box)


        self.stacked_layout.addWidget(self.finalPage)
    
    def client_clicked(self, indx):
        if indx < len(self.possible_clients):
            self.client = self.possible_clients[indx]
            self.get_study_id()
            self.id_edit.setText(self.study_id)
            self.get_description()
            self.desc_edit.setText(self.desc)
            self.formulation = f'To be provided by {self.client} or Melior'
            self.form_edit.setText(self.formulation)
        else:
            self.client = None
    
    def client_added(self):

        entered_text = self.client_widget.currentText()
        # self.label.setText(f"Entered: {entered_text}")
        self.client = entered_text
        self.get_study_id()
        self.id_edit.setText(self.study_id)
        self.get_description()
        self.desc_edit.setText(self.desc)
        self.formulation = f'To be provided by {self.client} or Melior'
        code = self.ref.get_client_code(self.client)
        if code is None:
            self.add_client_abbreviation()

    def on_date_changed(self, date):
        self.date = date
        self.get_study_id()
        self.id_edit.setText(self.study_id)

    def model_clicked(self, indx):
        if indx < len(self.possible_models):
            # print("Selected model:", self.possible_models[indx])
            self.model = self.possible_models[indx]
            self.get_study_id()
            self.id_edit.setText(self.study_id)
            self.get_description()
            self.desc_edit.setText(self.desc)

            
            self.methods[self.model]= None 
            for i in range(self.method_widget.count()):
                item = self.method_widget.item(i)
                if item.text() == self.model:  # Check if the item is in self.model
                    item.setSelected(True)
                else:
                    item.setSelected(False)
            
        else:
            self.model = None

        self.update_method_desc()
        # print(f'In model clicked: {self.methods}')

    def model_added(self):

        entered_text = self.model_widget.currentText()
        # self.label.setText(f"Entered: {entered_text}")
        self.model = entered_text
        self.get_study_id()
        self.id_edit.setText(self.study_id)
        self.get_description()
        self.desc_edit.setText(self.desc)
        code = self.ref.get_model_code(self.model)
        if code is None:
            self.add_method_dialog()

        # self.update_method_desc()

    def get_study_id(self):

        client_code = self.ref.get_client_code(self.client)
        if client_code is None:
            client_code = '---'

        self.api.filter_by_client(self.client)
        last_study = self.api.get_matching_db()['study_number'].max()
        if last_study is None:
            study_num = 1
        else:
            study_num = last_study +1
        self.api.create_filtered()
        
        # Format Date
        month_map = {
            1:'JAN', 2: 'FEB',3: 'MAR', 4: 'APR', 
            5: 'MAY',6: 'JUN',7: 'JUL',8: 'AUG',
            9 : 'SEPT', 10: 'OCT', 11: 'NOV', 12: 'DEC'
        }
        date_str = self.date.toString("dd") + month_map[self.date.month()]+ self.date.toString("yy")

        model_code = self.ref.get_model_code(self.model)
        if model_code is None:
            model_code = '---'

        self.study_id = client_code + '_'+ str(study_num).zfill(2) + '_'+ date_str + '_' + model_code

    def get_description(self):
        self.desc = str(self.client) + ' / ' + str(self.model)

    def next_page(self):
        if self.page_index < len(self.stacked_layout) -1:
            self.page_index = self.page_index + 1
            self.stacked_layout.setCurrentIndex(self.page_index)
    
    def last_page(self):
        if self.page_index > 0:
            self.stacked_layout.setCurrentIndex(self.page_index - 1)
            self.page_index = self.page_index - 1
        
    def species_clicked(self, indx):
        self.species = self.possible_species[indx]

    def strain_clicked(self, indx):
        if indx < len(self.possible_strains):
            self.strain = self.possible_strains[indx]
        else:
            self.strain = None

    def sex_clicked(self, indx):
        self.sex = self.possible_sexes[indx]

    def rand_state_changed(self, state):
        self.rand = state
        if self.rand:
            self.random_txt = 'Animals will be assigned randomly to treatment groups'
        else:
            self.random_txt = 'The study will not be randomized'
        self.random_edit.setText(self.random_txt)
    
    def blind_state_changed(self, state):
        self.blind = state
        if self.blind:
            self.blind_txt = 'The study will be blinded'
        else:
            self.blind_txt = 'The study will not be blinded'
        self.blind_edit.setText(self.blind_txt)
    
    def update_variable(self, text, variable_name):
        # Update the specified variable
        setattr(self, variable_name, text)
        # print(f"{variable_name} updated to:", getattr(self, variable_name))

    def update_numbers(self, text, variable_name):
        setattr(self, variable_name, text)

        for row in range(self.row_num):
            for col in range(len(self.table_cols)):
                if (self.table_cols[col] == "Group Size") and (self.animal_groups is not None):
                    self.tbl_contents[row][col] = self.animal_groups
                
        try:
            ag = int(self.animal_groups)
        except Exception as e:
            ag = None

        try:
            ng = int(self.number_groups)
            # self.row_num = ng
            self.row_count.setValue(ng)
            self.update_row_count()
        except Exception as e:
            ng = None

 
        if ag and ng:
            na = ag * ng
            self.number_animals = str(na)
            self.total_edit.setText(self.number_animals)
    
    def pm_clicked(self, indx):
        if indx < len(self.possible_employees):
            self.pm = self.possible_employees[indx]
        else:
            self.pm = None
    
    def pc_clicked(self, indx):
        if indx < len(self.possible_employees):
            self.pc = self.possible_employees[indx]
        else:
            self.pc = None

    def cms_clicked(self, indx):
        if indx < len(self.possible_employees):
            self.cms = self.possible_employees[indx]
        else:
            self.cms = None

    def method_choice(self):
        methods = [item.text() for item in self.method_widget.selectedItems()]
        # if len(methods) > 0:
        #     self.methods = {m: None for m in methods}
        for m in methods:
            if not m in self.methods.keys():
                self.methods[m] = None
        
        self.methods = {key: value for key, value in self.methods.items() if key in methods}

        
        self.update_method_desc()
        # print(f'in method choice: {self.methods}')

    def method_update(self):
        method = self.method_add.text()
        if len(method) == 0:
            pass
        else:
            self.method_widget.insertItem(1, method)
            self.method_widget.item(1).setSelected(True)
            self.method_widget.item(0).setSelected(False)
            
            self.method_add.setText(None)
            # self.new_items = True
        
        # print(f'in method update: {self.methods}')
        self.update_method_desc()

    def col_choice(self, state):

        checkbox = self.sender() 
        column_name = checkbox.text()
        # print(column_name)
        if state and (column_name not in self.table_cols):
            self.table_cols.append(column_name)
            for l in self.tbl_contents:
               l.append('')
             
        if not state and (column_name in self.table_cols):
            indx = self.table_cols.index(column_name)
            self.table_cols.remove(column_name)
            for l in self.tbl_contents:
                l.pop(indx)


        self.table.setColumnCount(len(self.table_cols))
        self.table.setHorizontalHeaderLabels(self.table_cols)
        self.update_table()

    def update_row_count(self):
        old_count = self.row_num
        difference = self.row_count.value() - old_count
        self.row_num = self.row_count.value()
        # self.table = QTableWidget(self.row_num, len(self.table_cols))
        self.table.setRowCount(self.row_num)
        self.table.setVerticalHeaderLabels([str(i+1) for i in range(self.row_count.value())])

        if difference > 0:
            for row in range(difference):
                self.tbl_contents.append([])
                for col in range(len(self.table_cols)):
                    if self.table_cols[col] == "Group #":
                        self.tbl_contents[-1].append(str(row+1+old_count))
                    elif (self.table_cols[col] == "Group Size") and (self.animal_groups is not None):
                        self.tbl_contents[-1].append(str(self.animal_groups))
                    else:
                        self.tbl_contents[-1].append('')
        elif difference < 0:
            self.tbl_contents = self.tbl_contents[:self.row_num]

        self.update_table()
        
        # print(f'Rows: {len(self.tbl_contents)}, Columns: {len(self.tbl_contents[0])}')

    def update_table(self):
        for row in range(self.row_num):
            for col in range(len(self.table_cols)):
                # if self.tbl_contents[row][col] != '':
                self.table.setItem(row, col, QTableWidgetItem(self.tbl_contents[row][col]))

    def cell_changed(self, row, column):
        item = self.table.item(row, column)
        self.tbl_contents[row][column] = item.text()

        # print(self.tbl_contents)
    
    def format_attributes(self):
        
        for key, value in self.methods.items():
            if value is None:
                self.methods[key] = f'Add method description for {key} here'
                # print(f'{key} changed to placeholder text')
        # print(self.methods)
        full_dict = {}
        full_dict['title'] = self.title
        full_dict['partner_company'] = self.client
        full_dict['d'] = self.date.toString("MMMM d, yyyy")
        full_dict['study_id'] = self.study_id
        full_dict['description'] = self.desc
        full_dict['species'] = self.species
        full_dict['strain'] = self.strain
        full_dict['source'] = self.source
        full_dict['age'] = self.age
        full_dict['sex'] = self.sex
        full_dict['randomization'] = self.random_txt
        full_dict['blinding'] = self.blind_txt
        full_dict['acc'] = self.acc
        full_dict['light_cycle'] = self.light_cycle
        full_dict['mice_per_cage'] = self.mice_per_cage
        full_dict['diet_water'] = self.diet_water
        full_dict['route'] = self.route
        full_dict['dose_volumes'] = self.dose_vol
        full_dict['formulation'] = self.formulation
        full_dict['dose_levels'] = self.dose_level
        full_dict['dose_frequency'] = self.dose_freq
        full_dict['study_duration'] = self.duration
        full_dict['pretreatment'] = self.pretreatment
        full_dict['num_groups'] = self.number_groups
        full_dict['num_per_group'] = self.animal_groups
        full_dict['total_animals'] = self.number_animals
        full_dict['col_labels'] = self.table_cols
        full_dict['tbl_contents'] = self.tbl_contents
        full_dict['methods'] = self.methods
        full_dict['pm'] = self.pm
        full_dict['pc'] = self.pc
        full_dict['cms'] = self.cms

         
        return full_dict
    
    def cheeky_dialog(self, text, title):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setText(text)
        msg.setWindowTitle(title)
        msg.setStandardButtons(QMessageBox.Ok)

        # Display the message box
        msg.exec_()

    def add_client_abbreviation(self):
        self.cc_dlg = QDialog(self)
        self.cc_dlg.setWindowTitle("Adding a new client")

        dlg_layout = QVBoxLayout()
        text = QLabel(f'Client Name: {self.client}')
        dlg_layout.addWidget(text)

        self.client_abb_input = QLineEdit()
        self.client_abb_input.setPlaceholderText('Enter abbreviation here')
        dlg_layout.addWidget(self.client_abb_input)

        enter_button = QPushButton("Save")
        enter_button.clicked.connect(self.update_client_codes)
        dlg_layout.addWidget(enter_button, alignment=Qt.AlignRight)
        

        self.cc_dlg.setLayout(dlg_layout)
        self.cc_dlg.exec()

    def add_method_dialog(self):
        self.method_dlg = QDialog(self)
        self.method_dlg.setWindowTitle("Adding a new model")

        dlg_layout = QVBoxLayout()
        text = QLabel(f'Model Name: {self.model}')
        dlg_layout.addWidget(text)

        self.model_abb_input = QLineEdit()
        self.model_abb_input.setPlaceholderText('Enter abbreviation here')
        dlg_layout.addWidget(self.model_abb_input)

        enter_button = QPushButton("Save")
        enter_button.clicked.connect(self.update_model_codes)
        dlg_layout.addWidget(enter_button, alignment=Qt.AlignRight)
        

        self.method_dlg.setLayout(dlg_layout)
        self.method_dlg.exec()

    def save_file(self):
        
        full_dict = self.format_attributes()
        # Open a file dialog to save a file
        options = QFileDialog.Options()
        if self.save_path is None:
            save_path = self.study_id
        else:
            save_path = self.save_path
        file_name, _ = QFileDialog.getSaveFileName(self, "Save File", save_path, "Word Files (*.docx);;All Files (*)", options=options)
        if file_name: 
            fill_proposal_template(full_dict, save_path = file_name)
            self.save_path = file_name
            self.cheeky_dialog('File saved successfully!', 'Congrats')

    def update_client_codes(self):
        code = self.client_abb_input.text()
        primary, scraping = self.ref.check_client_code(code)

        if len(primary):
            self.cheeky_dialog(f"{code} is the code for {primary['client'].item()}", "OOPS")
        elif len(scraping):
            clients = scraping['client'].to_list()
            if len(clients) == 1:
                clients = clients[0]
            self.cheeky_dialog(f'{code} is associated with {clients} on some documents', "OOPS")
        else:
            self.ref.add_new_client(self.client, code)
            self.possible_clients.append(self.client)
            self.cc_dlg.accept()
            self.get_study_id()
            self.id_edit.setText(self.study_id)
            self.new_items = True
            self.add_add_button()
    
    def update_model_codes(self):
        code = self.model_abb_input.text()
        matched = self.ref.check_model_code(code)

        if len(matched):
            self.cheeky_dialog(f"{code} is the code for {matched['method'].item()}", "OOPS")
        else:
            self.ref.add_new_method(self.model, code)
            self.possible_models.append(self.model)
            self.method_dlg.accept()
            self.get_study_id()
            self.id_edit.setText(self.study_id)
            self.new_items = True
            self.add_add_button()
            
    def save_new_codes(self):
        # print('saving tables')
        self.ref.save_tbl('methods')
        self.ref.save_tbl('clients')

    def add_add_button(self):
        if self.added is False:
            update_button = QPushButton("Add new Client/Model to database")
            update_button.clicked.connect(self.save_new_codes)
            self.start_layout.addWidget(update_button, alignment=Qt.AlignLeft)
            self.added = True  

    def update_method_desc(self):
        self.text_edits = {}    
        self.clear_layout(self.description_box)
        for key, value in self.methods.items():
            label = QLabel(key)
            self.text_edits[key] = QTextEdit()
            if value is None:
                self.text_edits[key].setPlaceholderText(f"Enter or copy text here...")
            else:
                self.text_edits[key].setPlainText(value)
            # self.text_edits[key] = text_edit
            self.text_edits[key].textChanged.connect(lambda k = key: self.update_text(k))
            search_button = QPushButton(f'Search for {key}')
            search_button.clicked.connect(partial(self.open_search_window, key))
            
            item_layout = QVBoxLayout()
            item_layout.addWidget(label)
            item_layout.addWidget(self.text_edits[key])
            item_layout.addWidget(search_button, alignment=Qt.AlignRight)
            self.description_box.addLayout(item_layout)

    def clear_layout(self, layout):
        """Clear all widgets from a layout."""
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
            elif item.layout() is not None:
                nested_layout = item.layout()
                self.clear_layout(nested_layout)

    def open_search_window(self, item):
        # Pass the `item` to the new window
        self.new_window = SearchWindow(item, self)
        self.new_window.show()

    def update_text(self, key):
        # Update the variable whenever the text is changed
        self.methods[key] = self.text_edits[key].toPlainText()
        # print(f"Method {key} has text: {self.methods[key]}")

class SearchWindow(QWidget):

    def __init__(self, item, mw):
        super().__init__()
        self.api  = searchingAPI("/Volumes/Company/Becca/Study Database/Database")
        self.api.create_filtered()
        self.mw = mw
        self.item = item
        # Set up window properties
        self.setWindowTitle(f"Search Window for {item}")
        self.resize(400, 300)
        self.filepaths = []
        # Create stacked layout
        self.stacked_layout = QStackedLayout()

        # Add pages to the stacked layout
        self.add_start_page(self.item)
        self.add_fileDisplay()

        # Main layout of the window
        main_layout = QVBoxLayout()
        main_layout.addLayout(self.stacked_layout)

        # Set the main layout
        self.setLayout(main_layout)

    def add_start_page(self, item):
        # Page one should offer radio/check buttons to search by
        # species, sex, strain, client
        page1 = QWidget()
        page1_layout = QVBoxLayout()


        checkboxes = QVBoxLayout()
        page1_label = QLabel("Choose additional parameters to search by:")
        checkboxes.addWidget(page1_label)

        
        self.species_check = QCheckBox('species')
        self.species_check.setChecked(False)
        # self.species_check.stateChanged.connect(lambda state: self.col_choice(state))
        checkboxes.addWidget(self.species_check, alignment = Qt.AlignTop)

        self.sex_check = QCheckBox('sex')
        self.sex_check.setChecked(False)
        # self.sex_check.stateChanged.connect(lambda state: self.col_choice(state))
        checkboxes.addWidget(self.sex_check, alignment = Qt.AlignTop)

        self.strain_check = QCheckBox('strain')
        self.strain_check.setChecked(False)
        # self.strain_check.stateChanged.connect(lambda state: self.col_choice(state))
        checkboxes.addWidget(self.strain_check, alignment = Qt.AlignTop)

        self.client_check = QCheckBox('client')
        self.client_check.setChecked(False)
        # self.client_check.stateChanged.connect(lambda state: self.col_choice(state))
        checkboxes.addWidget(self.client_check, alignment = Qt.AlignTop)
        page1_layout.addLayout(checkboxes)

        page1.setLayout(page1_layout)
        enter_button = QPushButton("Search")
        enter_button.clicked.connect(self.search_docs)
        page1_layout.addWidget(enter_button, alignment=Qt.AlignRight)


        self.stacked_layout.addWidget(page1)

    def add_fileDisplay(self):
        fileStack = QWidget()
        fileStackLayout = QVBoxLayout(fileStack)
  

        comp_count = QLabel(str(len(self.filepaths)) + " documents found")
        fileStackLayout.addWidget(comp_count)
        
        fileDisplay = QWidget()
        scroll = QScrollArea()
        self.display_layout = QVBoxLayout(fileDisplay)
        self.link_labels = []
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setWidgetResizable(True)
        scroll.setWidget(fileDisplay)
        fileStackLayout.addWidget(scroll)
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.last_page)
        fileStackLayout.addWidget(back_button, alignment=Qt.AlignRight)
        
        self.stacked_layout.addWidget(fileStack)

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
        # QDesktopServices.openUrl(QUrl(url))
        pass

    def last_page(self):
        self.clearLayout(self.display_layout)
        self.stacked_layout.setCurrentIndex(0)
        self.api.create_filtered()

    def clearLayout(self, layout):
        if layout is not None:
            while layout.count():
                child = layout.takeAt(0)
                if child.widget() is not None:
                    child.widget().deleteLater()
                elif child.layout() is not None:
                    self.clearLayout(child.layout())

    def search_docs(self):    
        
        self.api.create_filtered()
        self.api.filter_by_method(self.item)
        
        if self.api.check_filter_empty():
            QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
            return
        
                
        if (self.mw.species is not None) and (self.species_check.isChecked()):
            print(self.mw.species)
            self.api.filter_by_species(self.mw.species.lower())
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
        
        if (self.mw.strain is not None) and (self.strain_check.isChecked()):
            self.api.filter_by_strain(self.mw.strain)
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return


        if (self.mw.sex is not None) and (self.sex_check.isChecked()):
            self.api.filter_by_sex(self.mw.sex.lower())
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
            
        if (self.mw.client is not None) and (self.client_check.isChecked()):
            self.api.filter_by_client(self.mw.client)
            if self.api.check_filter_empty():
                QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
                return
        


            

        # self.api.filter_by_date((self.lower_year, self.upper_year))
        # if self.api.check_filter_empty():
        #         QMessageBox.information(self, "No Matches Found", "No documents matched criteria")
        #         return

        
        #     # self.found_docs = fin

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
    
        # self.comp_count.setText(str(len(self.filepaths)) + " studies found:")
        self.stacked_layout.setCurrentIndex(1)



if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())




