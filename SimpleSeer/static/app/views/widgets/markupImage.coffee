SubView = require '../subview'
application = require '../../application'
template = require './templates/markupImage'

module.exports = class markupImage extends SubView
  template: template
  className:"widget-markupImage"

  initialize: =>
  
  getRenderData: =>
    if @model
      {imgfile:@model.id}
  
  afterRender: =>
    @openUpExpanded()
  
  openUpExpanded: () =>
    #if @lastModel is model
    #  @hideImageExpanded()
    #  @lastModel = ""
    #  return
      
    #$@el.find(".image-view-item").addClass("currentExpanded");
    
    #thumbnail = element.find(".thumb")
    #offsetLeft = thumbnail.offset().left + thumbnail.width() + 37
    #imgWidth = thumbnail.parents("#views").width() - offsetLeft + 61
    
    offsetLeft = 0
    imgWidth = 600
    @$el.find("img").attr("src", @model.get('imgfile'))
    $("#viewStage").css({"left": offsetLeft + "px", "width": imgWidth + "px", "display": "block"}).removeClass("fixit");

    framewidth = @model.get("width")
    realwidth = imgWidth
    scale = realwidth / framewidth   

    @pjs = new Processing(@$el.find("canvas").get 0)
    @pjs.background(0,0)
    @pjs.size @$el.width(), @model.get("height") * scale
    @pjs.scale scale
    
    $("#displaycanvas").height(@model.get("height") * scale)
    console.log @model.get('features')
    if @model.get('features') then @model.get('features').each (f) => f.render(@pjs)
    #@viewIsScrolled()
    #@lastModel = @model
