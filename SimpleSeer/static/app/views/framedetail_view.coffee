View = require './view'
template = require './templates/framedetail'
application = require('application')

module.exports = class FrameDetailView extends View  
  template: template

  initialize: (frame)=>
    super()
    for k in application.settings.ui_metadata_keys
      if !frame.model.attributes.metadata[k]?
        frame.model.attributes.metadata[k] = ''
    @frame = frame.model
  
  events:
    'click #toggleProcessing' : 'togglePro'
    'click .clickEdit'  : 'switchStaticMeta'
    'blur .clickEdit'  : 'switchInputMeta'
    'change .notes-field' : 'updateNotes'

  togglePro: =>
    $("#displaycanvas").toggle();
    
  zoom: (e, ui) ->
    scale = $("#zoomer").data("orig-scale")
    os = $('#display').offset()
    viewPort = $('#display-zoom')
    
    if ui.zoom is 1
      @zoomed = false
      viewPort.css({
        'position': 'static',
        'left': 0,
        'top': 0,
        'width': '100%',
        'height': '100%'
      });
    else
      @zoomed = true
      viewPort.css({
        'position': 'relative',
        'top': '-'+(@.model.attributes.height * ui.zoom * ui.y)+'px',
        'left': '-'+(@.model.attributes.width * ui.zoom * ui.x)+'px',
        'width': (@.model.attributes.width * ui.zoom)+'px',
        'height': (@.model.attributes.height * ui.zoom)+'px',
      });
      $('#display').css("height", (@.model.attributes.height * scale))
      
  getRenderData: =>
    data = {}
   
    if @model.get("features").length
      data.featuretypes = _.values(@model.get("features").groupBy (f) -> f.get("featuretype"))
    
    for k of @model.attributes
      data[k] = @model.attributes[k]
    data.disabled = application.settings.mongo.is_slave || false

    md = @frame.get('metadata')
    metadata = []
    for i in application.settings.ui_metadata_keys
      metadata.push {key:i,val:md[i]}
    data.metadata = metadata
      
    data
    
  addMetaBox: =>
    return
    disabled = application.settings.mongo.is_slave || false
    html='<tr><td><input class="metaDataEdit" type="text"'
    if disabled
      html+=' disabled="disabled" '
    html+='></td><td><input class="metaDataEdit" type="text"'
    if disabled
      html+=' disabled="disabled" '    
    html+='></td></tr>'
    $('#metadata').append(html)

  updateMetaData: (self) =>  
    metadata = {}
    
    rows = $(self).find("tr")
    rows.each (id, obj) ->
      tds = $(obj).find('td')
      input = $(tds[1]).find('input')
      span = $(tds[0]).find('span')
      metadata[$(span).html()] = input.attr('value')
    
    #@addMetaBox(self)
    @model.save {metadata: metadata}

  updateNotes: (e) =>
    @model.save {notes:$(".notes-field").attr('value')}

  switchStaticMeta: (e) =>
    self = $(e.currentTarget)

    if self.find("input").length == 0
      $(self).html "<input type=\"text\" value=\"" + self.html() + "\">"
      self.find("input").focus()

  switchInputMeta: (e) =>
    target = $(e.currentTarget).parent().parent()

    #unless target.find("input").length is 0
    #  target.find("td").each (id, obj) ->
    #    $(obj).html $(obj).find("input").attr("value")

    #@delBlankMeta(target)
    @updateMetaData(target)

  calculateScale: =>
    framewidth = @model.get("width")
    realwidth = $('#display').width()
    scale = realwidth / framewidth

    scale

  updateScale: =>
    scale = @calculateScale()
    if scale is $("#zoomer").data("orig-scale")
      return
      
    $("#zoomer").data("orig-scale", scale)
    $("#zoomer").zoomify("option", {
      min: (scale.toFixed(2)) * 100,
      max: 400,
      zoom: scale.toFixed(2),
    })
  
  postRender: =>
    @addMetaBox()
    scale = @calculateScale()
  
    $("#zoomer").zoomify({
      image: @model.get('imgfile'),
      min: (scale.toFixed(2)) * 100,
      max: 400,
      zoom: scale.toFixed(2),
      update: (e, ui) =>
        @zoom(e, ui)
    }).data("orig-scale", scale)

    $(window).resize =>
      @updateScale()
        
    if not @model.get('features').length
      return
      
    @$(".tablesorter").tablesorter()
    @pjs = new Processing("displaycanvas")
    Document.pjs = @pjs
    @pjs.background(0,0)
    @pjs.size $('#display-img').width(), @model.get("height") * scale
    @pjs.scale scale
    @model.get('features').each (f) => f.render(@pjs)

    @$el.find(".notes-field").autogrow();
