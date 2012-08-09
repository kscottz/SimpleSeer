SubView = require '../subview'
application = require '../../application'
template = require './templates/tableView'

module.exports = class tableView extends SubView
  tableData: []
  columnOrder: []
  emptyCell: ""
  template:template

  events:
    'click #excel_button':'export'
    'click #csv_button':'export'

  initialize: =>
    @emptyCell = @options.emptyCell if @options.emptyCell
    @columnOrder = @options.columnOrder if @options.columnOrder
    super()

  empty: =>
    @tableData = []

  addRow: (row) =>
    newRow = {}
    for i,o of row
      if @isEmpty o
        o = @emptyCell
      newRow[i] = o
    @tableData.push newRow
    
  export: (ui) =>
    @$el.find('input[name="format"]').attr('value',ui.target.value)
    true

  getType: (val) =>
    """
    56%
    percent = if ends with %
    int = if is int
    date = if typeof date (if !moment, switch to moment)
    """

  isEmpty: (val) =>
    val == false || val == ''

  afterRender: =>
    super()
    l = @$el.find('thead :visible th')
    for dn in l
      if dn.innerHTML == "Capture Time"
        dn.innerHTML += " " + new Date().toString().match(/\(.*\)/g)
    @$el.find('.tablesorter').tablesorter({widgets: ['zebra']})
    js = @rows
    js.unshift @header
    $("input[name=rawdata]").attr('value',(JSON.stringify js).replace RegExp(@emptyCell,'g'), '' )
    #@$el.find(".scrollbar").tinyscrollbar({ axis: 'x'})


  getRenderData: =>
    retHeader = []
    retRow = []
    rr = []
    
    #populate initial coloumn order
    for col in @columnOrder
      retHeader.push col
      
    #populate row data
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
      
    #fill each row with empty cells if needed
    for a in rr
      while a.length < retHeader.length
        a.push @emptyCell
      retRow.push a

    @header = retHeader
    @rows = retRow
    return {header:retHeader,row:retRow}
    
