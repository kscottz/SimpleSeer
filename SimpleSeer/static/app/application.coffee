# The application bootstrapper.
Application =
  initialize: ->
    if @settings.mongo.is_slave
      $(".notebook").hide()
      
    ViewHelper = require 'lib/view_helper'
    HomeView = require 'views/home_view'
    FramelistView = require 'views/framelist_view'
    FrameDetailView = require 'views/framedetail_view'
    #FrameSetView = require 'views/frameset_view'
    Router = require 'lib/router'
    Inspections = require 'collections/inspections'
    Measurements = require 'collections/measurements'
    Frames = require 'collections/frames'
    OLAPs = require 'collections/OLAPs'
    FrameSets = require 'collections/framesets'

    @.socket = io.connect '/rt'
    @.socket.on 'connect', ->
      #alert 'connect'
    @.socket.on 'error', ->
      #alert 'error'
    @.socket.on 'disconnect', ->
      #alert 'disconnect'
    #@.socket.on 'message', (msg) ->
    #  console.log 'Got message', msg

    @inspections = new Inspections()
    @inspections.fetch()
    @charts = new OLAPs()
    @measurements = new Measurements()
    @measurements.fetch()
    @frames = new Frames()
    @framesets = new FrameSets()

    @lastframes = new Frames()

    @homeView = new HomeView()
    @framelistView = new FramelistView(@lastframes)

    # set up the timeout message dialog
    $('#lost_connection').dialog
      autoOpen: false
      modal: true
      buttons:
        Ok: ->
          $( this ).dialog( "close" )

    # Instantiate the router
    @router = new Router()
    # Freeze the object
    Object.freeze? this

  alert: (message, alert_type) ->
    _set = true
    $(".alert_"+alert_type).each (e,v)->
      if v.innerHTML == message
        _set = false
    if _set
      $("#messages").append('<div class="alert alert_'+alert_type+'">'+message+'</div>')

module.exports = Application
