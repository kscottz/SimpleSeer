SubView = require '../subview'
application = require '../../application'
template = require './templates/markupImage'

module.exports = class markupImage extends SubView
  model: ""
  pjs: ""
  zoom: 1
  template: template
  className:"widget-markupImage"
    
  getRenderData: =>
    data = {url: ""}
    if @model then data.url = "/grid/imgfile/" + @model.get("id")
    data
      
  afterRender: =>
    @renderProcessing()
      
  renderProcessing: =>
    if @model
      scale = @$el.width() / @model.get("width")
      
      @pjs = new Processing(@$el.find("canvas").get 0)
      @pjs.background(0,0)
      @pjs.size @$el.width(), @model.get("height") * scale
      @pjs.scale scale
      
      if !@model.get('features').length
        @model.get('features').each (f) => f.render(@pjs)
  
  setModel: (model) =>
    @model = model
    @render()
    
  setZoom: (zoom) =>
    @zoom = zoom
    @render()
