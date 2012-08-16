SubView = require '../subview'
application = require '../../application'
template = require './templates/frameViewer'

module.exports = class frameViewer extends SubView
  className:"frameViewer"
  tagName:"div"
  template:template
  
  initialize: =>
    super()
    application.socket.on "message:frame/", @capEvent
    application.socket.emit 'subscribe', 'frame/'

  capEvent:(frame)=>
    img = new Image()
    img.src = frame.data.imgfile
    $(img).load =>
      @url = frame.data.imgfile
      @$el.find('img').attr('src',@url)
    
  render:=>
    super()
  
  getRenderData: =>
    url:@url
  
  onUpdate: (frame) =>
    @frame = frame
    @render()
