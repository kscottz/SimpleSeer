template = require './templates/data'
application = require '../../application'
Tab = require '../tab_view'
tableView = require '../widgets/tableView'
FramelistFrameView = require '../framelistframe_view'


module.exports = class Data extends Tab
  template: template
  title:'Data View'
  initialize: () =>
    super()
    #@options.parent.filtercollection.on 'add', @addObj
    @options.parent.filtercollection.on 'reset', @addObjs

  select: =>
    super()
    @options.parent.filtercollection.limit = 65536
    @options.parent.filtercollection.skip = 0
    @options.parent.filtercollection.fetch
      before: @options.parent.preFetch
      success: () =>
        $('#data-views-controls').show()
        $('#views-contain').addClass('wide scroll')
        $('#views').addClass('wide')
        $('#content').addClass('wide')
        @options.parent.postFetch()

  render: () =>
    @tableView = @addSubview 'tabDataTable', tableView, '#tabDataTable',
      emptyCell:'---'
      columnOrder:["Capture Time","Left Fillet Angle&deg;","Right Fillet Angle&deg;","Part Number","Lot Number","Machine Number","First / Last Piece","Operator"]
    super()

  addObjs: (d)=>
    if @selected
      #console.log 'add to data'
      an = @$el.find('#frame_holder')
      if @options.parent.filtercollection.skip == 0
        an.html ''
      @$el.find('#count_viewing').html @options.parent.filtercollection.length
      @$el.find('#count_total').html @options.parent.filtercollection.totalavail    
      resort = true
      @$el.find("#tabDataTable").find('tbody').html('')
      @tableView.empty()
      for o in d.models
        fv = new FramelistFrameView {model:o}
        #fv.renderTableRow()
        fv.renderTableRow(@tableView)
        #@$el.find("#tabDataTable").find('tbody')
        #  .append(row) 
        #  .trigger('addRows', [row, resort]); 
      #@$el.find("#tabDataTable").trigger('update')
      @tableView.render()
  
