View = require './view'
template = require './templates/framelistframe'
application = require('application')

# FrameListFrame view that is displayed in the Frame
# List collection view. Shows an at-a-glance view of
# a frame and it's feature data.

module.exports = class FramelistFrameView extends View
  template: template
  className:'image-view-item'
  
  # Insert blank value pairs for non-existant
  # keys in the metadata.
  initialize: (frame)=>
    super()
    if !frame.model.attributes.metadata
      frame.model.attributes.metadata = {}
    for k in application.settings.ui_metadata_keys
      if !frame.model.attributes.metadata[k]?
        frame.model.attributes.metadata[k] = ''
    @frame = frame.model

  # Events for dirty input fields and handling
  # the display of the save buttons.
  events:
    'click .clickEdit'  : 'switchStaticMeta'
    'blur .clickEdit'  : 'switchInputMeta'
    'click .notes-field' : 'setDirty'
    'change .notes-field' : 'updateNotes'
    'click .savebtn' : 'setSaved'
    'focus .ivi-right' : 'showSaved'
    'blur .ivi-right' : 'hideSaved'

  # Opens up the markup-view created in the Imgae
  # tab.
  expandImage: =>
    application.framelistView.showImageExpanded @$el, @frame, @model
    @$el.find('.featureLabel').show()
  
  # Hide the feature data label.
  hideImage: =>
    @$el.find('.featureLabel').hide()
  
  # Show the save button to indicate dirty
  # changes.
  showSaved: =>
    @$el.find('.savebtn').show()
    
  # Hides the save button to indicate clean
  # input and db.
  hideSaved: =>
    @$el.find('.savebtn').hide()
    
  # Sets the text of the save button to indicate
  # that the database has been updated already.
  setSaved: =>
    @$el.find('.savebtn').button( "option" , 'label' , 'Saved' )
    @$el.find('.savebtn').button('disable')
    
  # Sets the text of the save button to indicate
  # that there are dirty changes that need to
  # be saved to the db.
  setDirty: =>
    @$el.find('.savebtn').button('enable')
    @$el.find('.savebtn').button( "option" , 'label' , 'Save' )
    @$el.find('.savebtn').show()

  # Loop through the input table and update the db
  # with the new fields.
  updateMetaData: (self) =>  
    metadata = {}
    rows = $(self).find("tr")
    rows.each (id, obj) ->
      tds = $(obj).find('td')
      input = $(tds[1]).find('input')
      span = $(tds[0]).find('span')
      metadata[$(span).html()] = input.attr('value')
    @model.save {metadata: metadata,notes:$(".notes-field").attr('value')}
    @setSaved()

  # Save the notes field in the database.
  updateNotes: (e) =>
    @model.save {notes:$(".notes-field").attr('value')}
    @setSaved()

  # Called when the metadata input fields are
  # moused on to. Display is set to dirty.
  switchStaticMeta: (e) =>
    self = $(e.currentTarget)
    @setDirty()

  # Called when the metadat input fields lose
  # focus. Updates the database.
  switchInputMeta: (e) =>
    target = $(e.currentTarget).parent().parent()
    @updateMetaData(target)
    
  # Spits out the rendering data to the templating
  # engine. Turns metadata into a key dict.
  getRenderData: =>
    md = @frame.get('metadata')
    metadata = []
    for i in application.settings.ui_metadata_keys
      metadata.push {key:i,val:md[i]}
    retVal =
      capturetime: new moment(parseInt @frame.get('capturetime')).format("M/D/YYYY h:mm a")
      camera: @frame.get('camera')
      imgfile: @frame.get('imgfile')
      thumbnail_file: @frame.get('thumbnail_file')
      id: @frame.get('id')
      features: @frame.get('features')
      metadata: metadata
      width: @frame.get('width')
      height: @frame.get('height')
      notes: @frame.get('notes')
    return retVal

  # Initialize jQuery elements on the html and
  # set the default state of the save buttons.
  afterRender: =>
    @$el.find(".notes-field").autogrow()
    @$el.find('.savebtn').button()
    @$el.find('.savebtn').hide()
    @setSaved()
  
  # Renders the frame's feature data and capture
  # time into a false table that is displayed
  # with the other frame information.
  renderTableRow: (table) =>
    awesomeRow = []
    rd = @getRenderData()
    awesomeRow['Capture Time'] = rd.capturetime
    for i in rd.metadata
      awesomeRow[i.key] = i.val
    if rd.features.models
      f = rd.features.models[0].getPluginMethod(rd.features.models[0].get("featuretype"), 'metadata')()
    else
      f = {}
    pairs = {}
    for i,o of f
      awesomeRow[o.title + o.units] = o.value
    table.addRow(awesomeRow)
