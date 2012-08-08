SubView = require '../subview'
application = require '../../application'
template = require './templates/markupImage'

module.exports = class markupImage extends SubView
  template: template
  className:"widget-markupImage"

  initialize: =>
    $("#viewStage .close").live "click", =>
      $(".currentExpanded").removeClass("currentExpanded")
      $("#viewStage").hide()
    
    $(window).scroll =>
      if $(window).scrollTop() < 63
        $("#viewStage")
          .removeClass("fixit")
          .css("left", $("#viewStage").data("left") + "px")
      else
        $("#viewStage")
          .addClass("fixit")
          .css("left", $("#viewStage").data("left") + 101 + "px")
  
  getRenderData: =>
    if @model
      {imgfile: "/grid/imgfile/"+@model.get("id")}
    else
      {imgfile: ""}
  
  openUpExpanded: () =>
    if $($.find(".thumb")[0]).length is 0
      ""
      
    application.framelistView.hideMenu =>
      thumbnail = $($.find(".thumb")[0])
      offsetLeft = thumbnail.offset().left + thumbnail.width() - 41
      imgWidth = thumbnail.parents("#image_tab").width() - offsetLeft - 17
  
      framewidth = @model.get("width")
      realwidth = imgWidth
      scale = realwidth / framewidth
      
      $("#viewStage").css({"left": offsetLeft + "px", "width": imgWidth + "px", "display": "block"}).data("left", offsetLeft);
      $("#viewStage.fixit").css("left", $("#viewStage").data("left") + 101 + "px")
      
      $("#markupImageTarget").css("height", (@model.get("height") * scale) + "px")
      
      @pjs = new Processing(@$el.find("canvas").get 0)
      @pjs.background(0,0)
      @pjs.size @$el.width(), @model.get("height") * scale
      @pjs.scale scale
      if @model.get('features') then @model.get('features').each (f) => f.render(@pjs)
