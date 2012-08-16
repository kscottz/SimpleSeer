SubView = require '../subview'
application = require '../../application'
template = require './templates/tableView'

# TableView is a wrapper for the jQuery UI Tablesorter
# wigdet. Allows you to pass in data and draw it
# programmatically.

module.exports = class tableView extends SubView
  # Define some working variables.
  tableData: []
  columnOrder: []
  emptyCell: ""
  template:template

  # Event handlers for the export buttons.
  events:
    'click #excel_button':'export'
    'click #csv_button':'export'

  # Localizes the scope of the widget settings.
  initialize: =>
    @emptyCell = @options.emptyCell if @options.emptyCell
    @columnOrder = @options.columnOrder if @options.columnOrder
    super()

  # Clears the table.
  empty: =>
    @tableData = []

  # Add a row into the table.
  addRow: (row) =>
    newRow = {}
    for i,o of row
      if @isEmpty o
        o = @emptyCell
      newRow[i] = o
    @tableData.push newRow
    
  # Pushes the table data into a hidden
  # input form value.
  export: (ui) =>
    @$el.find('input[name="format"]').attr('value',ui.target.value)
    true

  # [NOT IMPLEMENTED]
  #  56%
  #  percent = if ends with %
  #  int = if is int
  #  date = if typeof date (if !moment, switch to moment) 
  getType: (val) =>
    
  # Helper function to return falsy table
  # data.
  isEmpty: (val) =>
    val == false || val == ''

  # Renders the table. Will replace the date
  # header with the current timezone.
  afterRender: =>
    super()
    
    l = @$el.find('thead :visible th')
    for dn in l
      if dn.innerHTML == "Capture Time"
        dn.innerHTML += " " + new Date().toString().match(/\(.*\)/g)
        
    js = @rows
    js.unshift @header
    $("input[name=rawdata]").attr('value',(JSON.stringify js).replace RegExp(@emptyCell,'g'), '' )
    @$el.find('.tablesorter').tablesorter({widgets: ['zebra']})

  # Builds a two dimensional array for the
  # template to render out. Will fill in
  # missing cells with a blank representation.
  getRenderData: =>
    retHeader = []
    retRow = []
    rr = []
    
    # Populate initial coloumn order.
    for col in @columnOrder
      retHeader.push col
      
    # Populate row data.
    for row in @tableData
      _r = []
      while _r.length < retHeader.length
        _r.push @emptyCell
      for col, rowItem of row
        i = retHeader.indexOf(col)
        if i == -1
          retHeader.push col
          i = retHeader.indexOf(col)
        _r[i] = rowItem
      rr.push _r
      
    # Fill each row with empty cells if needed.
    for a in rr
      while a.length < retHeader.length
        a.push @emptyCell
      retRow.push a

    @header = retHeader
    @rows = retRow
    return {header:retHeader,row:retRow}
    
