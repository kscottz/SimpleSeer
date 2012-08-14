SubView = require '../subview'
application = require '../../application'
template = require './templates/markupImage'

# MarkupImage is a subview / widget used for
# displaying an image with a canvas overlay
# that uses Processing.js for markup to the
# image.

module.exports = class markupImage extends SubView
  # Applied to the container that this
  # widget is initialized on.
  className:"widget-markupImage"
  
  # Define some working variables.
  model: ''
  pjs: ''
  zoom: 1
  template: template
  

  # Returns a blank image url if a model
  # if not defined yet. Otherwise, pull
  # in the fullsize view from the model.
  getRenderData: =>
    return {url: if @model then data.url = "/grid/imgfile/" + @model.get("id") else "" }
    
  # After the DOM is created we can play
  # with the canvas.
  afterRender: =>
    @renderProcessing()
  
  # If the model is defined and the DOM
  # for the widget is initialized, call
  # Processing.js and draw the features.
  renderProcessing: =>
    if @model
      scale = @$el.width() / @model.get("width")
      
      @pjs = new Processing(@$el.find("canvas").get 0)
      @pjs.background(0,0)
      @pjs.size @$el.width(), @model.get("height") * scale
      @pjs.scale scale
      
      if @model.get('features').length
        @model.get('features').each (f) => f.render(@pjs)
  
  # Setter function for the model. Will
  # re-render the view automatically.
  setModel: (model) =>
    @model = model
    @render()
  
  # Setter function for the zoom level.
  # Will re-render the canvas automatically.
  setZoom: (zoom) =>
    @zoom = zoom
    @renderProcessing()
