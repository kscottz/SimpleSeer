require 'lib/slide_replace'
application = require 'application'
#Frame = require "../models/frame"
#FrameDetailView = require "views/framedetail_view"
#FramelistView = require "views/framelist_view"
TabContainer = require "views/tabcontainer_view"

module.exports = class SeerRouter extends Backbone.Router
  routes: application.settings['ui_routes'] || {}

  #for route, name of application.settings['ui_navurls']
    #$('.nav').append '<li class="'+name.toLowerCase()+'"><a href="#'+route+'">'+name+'</a></li>'
  #if application.settings['ui_enablenotebook']
    #$('.nav').append '<li class="notebook"><a href=\'javascript: window.open(window.location.protocol + "//" + window.location.hostname + ":5050");\'>Develop</a></li>'

  home: ->
    return ""
