template = require './templates/image'
application = require '../../application'
Tab = require '../tab_view'
FramelistFrameView = require '../framelistframe_view'

module.exports = class Image extends Tab
  template: template
  title:'Image View'
  selected:true
  initialize: () =>
    super()
    @options.parent.filtercollection.on 'add', @addObj
    @options.parent.filtercollection.on 'reset', @addObjs
    $(window).on 'scroll', @loadMore

  select: =>
    super()
    @options.parent.filtercollection.limit = @options.parent.filtercollection._defaults.limit
    @options.parent.filtercollection.skip = @options.parent.filtercollection._defaults.skip
    @options.parent.filtercollection.fetch
      before: @options.parent.preFetch
      success: () =>
        $('#data-views-controls').hide()
        $('#views-controls').show()
        $('#views-contain').removeClass('wide')
        @options.parent.postFetch()

  addObj: (d)=>
    an = @$el.find('#frame_holder')
    fv = new FramelistFrameView {model:d}
    an.append(fv.render().el)

  addObjs: (d)=>
    if @selected
      #console.log 'add to image'
      an = @$el.find('#frame_holder')
      if @options.parent.filtercollection.skip == 0
        an.html ''
      for o in d.models
        fv = new FramelistFrameView {model:o}
        an.append(fv.render().el)
  
  loadMore: (evt)=>
    if ($(window).scrollTop() >= ($(document).height() - $(window).height() - 1)) && !application.isLoading()
      if (@options.parent.filtercollection.length+1) <= @options.parent.filtercollection.totalavail
        @$el.find('#loading_message').fadeIn('fast')
        @options.parent.filtercollection.skip += @options.parent.filtercollection._defaults.limit
        @options.parent.filtercollection.fetch({before: @options.parent.preFetch,success:@options.parent.postFetch})
        

