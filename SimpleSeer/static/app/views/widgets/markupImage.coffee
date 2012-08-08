SubView = require '../subview'
application = require '../../application'
template = require './templates/markupImage'

module.exports = class markupImage extends SubView
  template: template
  className:"widget-markupImage"

  initialize: =>
    $(window).scroll =>
      if $(window).scrollTop() < 128 
        $("#viewStage").removeClass("fixit");
      else
        $("#viewStage").addClass("fixit");        
  
  getRenderData: =>
    if @model
      {imgfile: "/grid/imgfile/"+@model.get("id")}
  
  afterRender: =>
    @openUpExpanded()
  
  openUpExpanded: () =>
    application.framelistView.hideMenu =>        
      #$@el.find(".image-view-item").addClass("currentExpanded");
      
      thumbnail = $($.find(".thumb")[0])
      offsetLeft = thumbnail.offset().left + thumbnail.width() - 41
      imgWidth = thumbnail.parents("#image_tab").width() - offsetLeft - 17
  
      framewidth = @model.get("width")
      realwidth = imgWidth
      scale = realwidth / framewidth
      
      $("#viewStage").css({"left": offsetLeft + "px", "width": imgWidth + "px", "display": "block"}).removeClass("fixit");
      $("#markupImageTarget").css("height", (@model.get("height") * scale) + "px")
  
      @pjs = new Processing(@$el.find("canvas").get 0)
      @pjs.background(0,0)
      @pjs.size @$el.width(), @model.get("height") * scale
      @pjs.scale scale
      if @model.get('features') then @model.get('features').each (f) => f.render(@pjs)
