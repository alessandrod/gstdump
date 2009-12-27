#!/usr/bin/env python
#
# This program is free software. It comes without any warranty, to
# the extent permitted by applicable law. You can redistribute it
# and/or modify it under the terms of the Do What The Fuck You Want
# To Public License, Version 2, as published by Sam Hocevar. See
# http://sam.zoy.org/wtfpl/COPYING for more details.
#
# Author: Alessandro Decina <alessandro.d@gmail.com>

import pygtk
pygtk.require("2.0")
import gobject
gobject.threads_init()

import pygst
pygst.require("0.10")
import gst

from twisted.internet import glib2reactor
glib2reactor.install()
from twisted.internet import reactor
from twisted.application.service import IService, Service
from twisted.python import log

EXIT_EOS = 0
EXIT_ERROR = 1
EXIT_LINK_TIMEOUT = 2

# time in seconds
LINK_TIMEOUT = 30

class Codec(object):
    def __init__(self, capsName, parser=None):
        self.capsName = capsName
        self.parser = parser

    def createParser(self):
        if self.parser is None:
            return gst.element_factory_make("identity")

        return gst.element_factory_make(self.parser)

class VideoCodec(Codec):
    codecType = "video"

class AudioCodec(Codec):
    codecType = "audio"

SUPPORTED_CODECS = [
        VideoCodec("video/x-h264", "h264parse"),
        VideoCodec("video/mpeg"),
        VideoCodec("image/jpeg"),
        VideoCodec("video/x-raw-rgb"),
        VideoCodec("video/x-raw-yuv"),
        AudioCodec("audio/mpeg"),
        AudioCodec("audio/x-raw-int", "audioparse")
    ]

def findCodecForCaps(caps, codecs):
    for structure in caps:
        for codec in codecs:
            if codec.capsName == structure.get_name():
                return codec

    return None

def supportedCodecCaps(caps, codecs):
    return bool(findCodecForCaps(caps, codecs))

class DumpError(Exception):
    pass

class DumpService(Service):
    def __init__(self, uri, outputFilename):
        self.uri = uri
        self.outputFilename = outputFilename
        self.pipeline = None
        self.exitStatus = None
        self.linkTimeoutCall = None

    def startService(self):
        if self.pipeline is not None:
            raise DumpError("dump already started")

        self.buildPipeline()
        self.pipeline.set_state(gst.STATE_PLAYING)
        self.startLinkTimeout()

        Service.startService(self)

    def stopService(self):
        self.pipeline.set_state(gst.STATE_NULL)

        Service.stopService(self)

    def busEosCb(self, bus, message):
        self.logInfo("gstreamer eos")
        self.shutdown(EXIT_EOS)

    def busErrorCb(self, bus, message):
        gerror, debug = message.parse_error()
        self.logError("gstreamer error %s %s" % (gerror.message, debug))
        self.shutdown(EXIT_ERROR)

    def decodebinAutoplugContinueCb(self, decodebin, pad, caps):
        continueDecoding = not supportedCodecCaps(caps, SUPPORTED_CODECS)
        return continueDecoding

    def decodebinPadAddedCb(self, decodebin, pad):
        caps = pad.get_caps()
        codec = findCodecForCaps(caps, SUPPORTED_CODECS)
        if codec is None:
            return

        self.maybeCancelLinkTimeout()

        self.logInfo("found %s codec %s" % (codec.codecType, codec.capsName))

        parser = codec.createParser()
        self.pipeline.add(parser)
        parser.link(self.qtmux)
        pad.link(parser.get_pad("sink"))
        parser.set_state(gst.STATE_PLAYING)

    def decodebinNoMorePadsCb(self, decodebin):
        self.logInfo("no more pads")
        self.qtmux.set_locked_state(False)
        self.qtmux.sync_state_with_parent()

    def buildPipeline(self):
        self.pipeline = gst.Pipeline()
        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self.bus.connect("message::eos", self.busEosCb)
        self.bus.connect("message::error", self.busErrorCb)

        self.uridecodebin = gst.element_factory_make("uridecodebin")
        self.uridecodebin.props.uri = self.uri
        self.uridecodebin.connect("autoplug-continue",
            self.decodebinAutoplugContinueCb)
        self.uridecodebin.connect("pad-added", self.decodebinPadAddedCb)
        self.uridecodebin.connect("no-more-pads", self.decodebinNoMorePadsCb)

        self.qtmux = gst.element_factory_make("qtmux")
        self.qtmux.set_locked_state(True)
        self.filesink = gst.element_factory_make("filesink")
        self.filesink.props.location = self.outputFilename

        self.pipeline.add(self.uridecodebin, self.qtmux, self.filesink)
        gst.element_link_many(self.qtmux, self.filesink)

    def cleanPipeline(self):
        self.pipeline.set_state(gst.STATE_NULL)
        self.pipeline = None

    def shutdown(self, exitStatus):
        exit = dict((v, k) for k, v in globals().iteritems()
                if k.startswith("EXIT_"))[exitStatus]
        self.logInfo("shutdown %s" % exit)
        self.exitStatus = exitStatus

        self.cleanPipeline()
        reactor.stop()

    def startLinkTimeout(self):
        assert self.linkTimeoutCall is None
        self.linkTimeoutCall = self.callLater(LINK_TIMEOUT,
                self.linkTimeout)

    def maybeCancelLinkTimeout(self):
        if self.linkTimeoutCall is not None:
            self.linkTimeoutCall.cancel()
            self.linkTimeoutCall = None

    def linkTimeout(self):
        self.logError("couldn't find any compatible streams")
        self.shutdown(EXIT_LINK_TIMEOUT)

    def callLater(self, seconds, callable, *args, **kwargs):
        return reactor.callLater(seconds, callable, *args, **kwargs)

    def logInfo(self, message, **kwargs):
        log.msg("info: %s" % message, **kwargs)

    def logError(self, message, **kwargs):
        kwargs["isError"] = True
        log.msg("error: %s" % message, **kwargs)

if __name__ == "__main__":
    from twisted.application.service import Application
    import sys

    log.startLogging(sys.stdout, setStdout=False)

    application = Application("GstDump")

    dump = DumpService(sys.argv[1], sys.argv[2])
    dump.setServiceParent(application)

    appService = IService(application)
    reactor.callWhenRunning(appService.startService)
    reactor.run()

    sys.exit(dump.exitStatus)
