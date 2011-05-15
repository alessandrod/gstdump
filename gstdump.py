#!/usr/bin/env python
#
# This program is free software. It comes without any warranty, to
# the extent permitted by applicable law. You can redistribute it
# and/or modify it under the terms of the Do What The Fuck You Want
# To Public License, Version 2, as published by Sam Hocevar. See
# http://sam.zoy.org/wtfpl/COPYING for more details.
#
# Author: Alessandro Decina <alessandro.d@gmail.com>

if __name__ == "__main__":
    from twisted.internet import glib2reactor
    glib2reactor.install()

import os

import pygtk
pygtk.require("2.0")
import gobject
gobject.threads_init()

import pygst
pygst.require("0.10")
import gst

from twisted.internet import reactor, defer, threads
from twisted.application.service import IService, Service
from twisted.python import log
from twisted.python.usage import Options as BaseOptions, UsageError

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_LINK_TIMEOUT = 2
EXIT_EOS_TIMEOUT = 3

# time in seconds
DEFAULT_LINK_TIMEOUT = 30
DEFAULT_EOS_TIMEOUT = 5
DEFAULT_MUXER = "qt"

muxerTemplates = {
    "qtmux": {"audio": "audio_%d", "video": "video_%d"},
    "flvmux": {"audio": "audio", "video": "video"}
}

def exitStatusName(exitStatus):
    try:
        exit = dict((v, k) for k, v in globals().iteritems()
                if k.startswith("EXIT_"))[exitStatus]
    except KeyError:
        exit = "UNKNOWN_ERROR: %s" % exitStatus
    return exit

def logInfo(message, **kwargs):
    log.msg("info: %s" % message, **kwargs)

def logError(message, **kwargs):
    kwargs["isError"] = True
    log.msg("error: %s" % message, **kwargs)

class Codec(object):
    def __init__(self, caps, binDescription=None):
        self.caps = gst.Caps(caps)
        self.binDescription = binDescription

    def createBin(self):
        bin_element = gst.Bin()
        codecBin = self.createBinReal()

        logTimestamps = LogTimestamps()
        bin_element.add(codecBin, logTimestamps)
        codecBin.link(logTimestamps)

        sinkpad = gst.GhostPad("sink", codecBin.get_pad("sink"))
        srcpad = gst.GhostPad("src", logTimestamps.get_pad("src"))
        bin_element.add_pad(sinkpad)
        bin_element.add_pad(srcpad)

        self.addProbes(bin_element)
        return bin_element

    def createBinReal(self):
        if self.binDescription is None:
            return gst.element_factory_make("identity")

        return gst.parse_bin_from_description(self.binDescription,
                ghost_unconnected_pads=True)

    def addProbes(self, codecBin):
        for padName in ("sink", "src"):
            pad = codecBin.get_pad(padName)
            pad.connect("notify::caps", self.padNotifyCaps)

    def padNotifyCaps(self, pad, pspec):
        caps = pad.props.caps
        logInfo("pad %s changed caps %s" % (pad, caps))

class VideoCodec(Codec):
    codecType = "video"

class AudioCodec(Codec):
    codecType = "audio"

class LogTimestamps(gst.Element):
    __gstdetails__ = ("LogTimestamps", "Filter",
            "Blah", "Alessandro Decina")

    sinktemplate = gst.PadTemplate ("sink",
            gst.PAD_SINK, gst.PAD_ALWAYS, gst.Caps("ANY"))
    srctemplate = gst.PadTemplate ("src",
            gst.PAD_SRC, gst.PAD_ALWAYS, gst.Caps("ANY"))

    def __init__(self):
        gst.Element.__init__(self)

        self.sinkpad = gst.Pad(self.sinktemplate)
        self.sinkpad.set_event_function(self.sink_event)
        self.sinkpad.set_setcaps_function(self.sink_setcaps)
        self.sinkpad.set_getcaps_function(self.sink_getcaps)
        self.sinkpad.set_chain_function(self.chain)
        self.add_pad(self.sinkpad)

        self.srcpad = gst.Pad(self.srctemplate)
        self.add_pad(self.srcpad)

        self.reset()

    def reset(self):
        self.segment = gst.Segment()
        self.log_ts = gst.CLOCK_TIME_NONE
        self.log_interval = 30 * gst.SECOND

    def sink_event(self, pad, event):
        if event.type == gst.EVENT_FLUSH_STOP:
            self.reset()

        return self.sinkpad.event_default(event)

    def sink_setcaps(self, pad, caps):
        return self.srcpad.set_caps(caps)

    def sink_getcaps(self, pad):
        peer = self.srcpad.get_peer()
        if peer is not None:
            return peer.get_caps()
        return self.srcpad.get_caps()

    def chain(self, pad, buf):
        ts = buf.timestamp
        if self.log_ts == gst.CLOCK_TIME_NONE or ts >= self.log_ts:
            self.log_ts = ts + self.log_interval
            logInfo("now at %s" % gst.TIME_ARGS(ts))

        return self.srcpad.push(buf)

gobject.type_register(LogTimestamps)
gst.element_register(LogTimestamps, "logtimestamps", gst.RANK_MARGINAL)


SUPPORTED_CODECS = [
        VideoCodec("video/x-h264", 'h264parse'),
        VideoCodec("video/mpeg, mpegversion=(int)4", "mpeg4videoparse"),
        VideoCodec("video/mpeg, mpegversion={1, 2}", "mpegvideoparse"),
        VideoCodec("image/jpeg"),
        VideoCodec("video/x-raw-rgb", "ffmpegcolorspace ! x264enc"),
        VideoCodec("video/x-raw-yuv", "ffmpegcolorspace ! x264enc"),
        #AudioCodec("audio/mpeg, mpegversion=(int){2, 4}", "aacparse"),
        AudioCodec("audio/mpeg, mpegversion=(int)1, layer=[1, 3]", "mp3parse"),
        AudioCodec("audio/x-raw-int", "lame"),
    ]

def findCodecForCaps(caps, codecs):
    for codec in codecs:
        if codec.caps.intersect(caps):
            return codec

    return None

def supportedCodecCaps(caps, codecs):
    return bool(findCodecForCaps(caps, codecs))

class DumpError(Exception):
    pass

class TimeoutCall(object):
    call = None

    def __init__(self, timeout, callback, *args, **kw):
        self.timeout = timeout
        self.callback = callback
        self.args = args
        self.kw = kw

    def callCallback(self, *args, **kw):
        self.call = None
        self.callback(*args, **kw)

    def start(self):
        assert self.call is None
        self.call = self.callLater(self.timeout, self.callCallback,
                *self.args, **self.kw)

    def cancel(self):
        if self.call is None:
            return

        self.call.cancel()
        self.call = None

    def callLater(self, timeout, callback, *args, **kw):
        return reactor.callLater(timeout, callback, *args, **kw)


class DumpService(Service):
    def __init__(self, uri, outputFilename, linkTimeout=None,
            eosTimeout=None, muxer=None):
        self.uri = uri
        self.outputFilename = outputFilename
        self.pipeline = None
        self.exitStatus = None
        if linkTimeout is None:
            linkTimeout = DEFAULT_LINK_TIMEOUT
        if eosTimeout is None:
            eosTimeout = DEFAULT_EOS_TIMEOUT
        self.linkTimeout = linkTimeout
        self.eosTimeout = eosTimeout
        if muxer is None:
            muxer = DEFAULT_MUXER + "mux"
        self.muxerFactory = muxer
        self.linkTimeoutCall = TimeoutCall(linkTimeout, self.linkTimeoutCb)
        self.eosTimeoutCall = TimeoutCall(eosTimeout, self.eosTimeoutCb)
        self.blockedPads = []
        self.inShutdown = False
        self.shutdownDeferred = None
        self.duration = gst.CLOCK_TIME_NONE

    def startService(self):
        assert self.shutdownDeferred is None
        self.shutdownDeferred = defer.Deferred()
        self.startPipeline()
        Service.startService(self)

    def startPipeline(self):
        if self.pipeline is not None:
            raise DumpError("dump already started")

        self.buildPipeline()

        self.linkTimeoutCall.start()
        self.pipeline.set_state(gst.STATE_PLAYING)

    def stopService(self, stopReactor=False):
        Service.stopService(self)
        if self.pipeline is not None:
            self.forceEos()
        else:
            self.shutdown(EXIT_OK, stopReactor)

        return self.shutdownDeferred

    def findDuration(self):
        try:
            duration, fmt = self.pipeline.query_duration(gst.FORMAT_TIME)
            self.duration = duration
        except gst.QueryError:
            pass

    def busEosCb(self, bus, message):
        logInfo("gstreamer eos")
        self.eosTimeoutCall.cancel()
        if self.muxerFactory == "qtmux":
            fast_start = self.muxer.props.faststart_file
        self.shutdown(EXIT_OK)
        if self.muxerFactory == "qtmux":
            os.unlink(fast_start)

    def busErrorCb(self, bus, message):
        gerror, debug = message.parse_error()
        logError("gstreamer %s %s" % (gerror.message, debug))
        self.shutdown(EXIT_ERROR)

    def decodebinAutoplugContinueCb(self, decodebin, pad, caps):
        if caps.is_fixed():
            continueDecoding = not supportedCodecCaps(caps, SUPPORTED_CODECS)
        else:
            continueDecoding = True

        logInfo("found stream %s, continue decoding %s" %
                (caps.to_string()[:500], continueDecoding))
        return continueDecoding

    def decodebinPadAddedCb(self, decodebin, pad):
        caps = pad.get_caps()
        codec = findCodecForCaps(caps, SUPPORTED_CODECS)
        if codec is None:
            return

        self.linkTimeoutCall.cancel()

        logInfo("found %s codec %s" % (codec.codecType, codec.caps.to_string()[:500]))

        queue = gst.element_factory_make("queue")
        queue.props.max_size_bytes = 0
        queue.props.max_size_time = 0
        queue.props.max_size_buffers = 0
        codecBin = codec.createBin()
        self.pipeline.add(codecBin, queue)
        codecBin.set_state(gst.STATE_PLAYING)
        queue.set_state(gst.STATE_PLAYING)

        try:
            pad.link(codecBin.get_pad("sink"))
            gst.element_link_many(codecBin, queue)
        except gst.LinkError, e:
            logError("couldn't link codecBin and queue %s" % e)
            self.callLater(0, self.shutdown, EXIT_ERROR)

            return

        queuePad = queue.get_pad("src")
        if codec.codecType == 'audio':
            name = muxerTemplates[self.muxerFactory]["audio"]
        else:
            name = muxerTemplates[self.muxerFactory]["video"]
        muxerPad = self.muxer.get_request_pad(name)
        try:
            queuePad.link(muxerPad)
        except gst.LinkError, e:
            logError("couldn't link to muxer %s" % e)
            self.callLater(0, self.shutdown, EXIT_ERROR)

            return

        queuePad.set_blocked_async(True, self.padBlockedCb)
        self.blockedPads.append(queuePad)

    def decodebinNoMorePadsCb(self, decodebin):
        logInfo("no more pads")
        self.muxer.set_locked_state(False)
        self.muxer.set_state(gst.STATE_PLAYING)

        blockedPads, self.blockedPads = self.blockedPads, []
        for pad in blockedPads:
            pad.set_blocked_async(False, self.padBlockedCb)

    def padBlockedCb(self, pad, blocked):
        if blocked:
            logInfo("blocked pad %s" % pad)
        else:
            logInfo("unblocked pad %s" % pad)

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

        self.muxer = gst.element_factory_make(self.muxerFactory)
        if self.muxerFactory == "qtmux":
            self.muxer.props.faststart = True
        self.muxer.set_locked_state(True)
        self.filesink = gst.element_factory_make("filesink")
        self.filesink.props.location = self.outputFilename

        self.pipeline.add(self.uridecodebin, self.muxer, self.filesink)
        gst.element_link_many(self.muxer, self.filesink)

    def cleanPipeline(self):
        self.pipeline.set_state(gst.STATE_NULL)
        self.pipeline = None
        self.linkTimeoutCall.cancel()
        self.eosTimeoutCall.cancel()

    def shutdown(self, exitStatus, stopReactor=True):
        if self.inShutdown:
            return

        self.inShutdown = True
        self.shutdownReal(exitStatus, stopReactor)
        self.inShutdown = False

    def shutdownReal(self, exitStatus, stopReactor=True):
        # shutdown() calls reactor.stop() which in turn calls
        # stopService(). So we need to call shutdown only if shutdown hasn't
        # been called yet.
        exit = exitStatusName(exitStatus)
        logInfo("shutdown %s" % exit)
        self.exitStatus = exitStatus

        self.cleanPipeline()

        dfr = self.shutdownDeferred
        self.shutdownDeferred = None
        dfr.callback(self)

        #if stopReactor:
        #    reactor.stop()

    def linkTimeoutCb(self):
        logError("couldn't find any compatible streams")
        self.shutdown(EXIT_LINK_TIMEOUT)

    def callLater(self, seconds, callable, *args, **kwargs):
        return reactor.callLater(seconds, callable, *args, **kwargs)

    def sigInt(self, sigNum, frame):
        reactor.callFromThread(self.forceEos)

    def sigUsr2(self, sigNum, frame):
        reactor.callFromThread(self.forceEos)

    def forceEos(self):
        if self.pipeline is None:
            return
        self.findDuration()
        logInfo("forcing EOS")
        self.eosTimeoutCall.start()
        self.doControlledShutdown()

    def doControlledShutdown(self):
        logInfo("doing regular controlled shutdown")
        #self.pipeline.send_event(gst.event_new_eos())
        for pad in self.muxer.sink_pads():
            threads.deferToThread(pad.send_event, gst.event_new_eos())

    def eosTimeoutCb(self):
        self.shutdown(EXIT_EOS_TIMEOUT)


class Options(BaseOptions):
    synopsis = ""
    optParameters = [
            ["timeout", "t", DEFAULT_LINK_TIMEOUT, "timeout"],
            ["muxer", "m", DEFAULT_MUXER, "muxer"],
        ]

    optFlags = [
            ["quiet", "q", "suppress regular output, show only errors"]
        ]

    def opt_timeout(self, timeout):
        try:
            self["timeout"] = int(timeout)
        except ValueError:
            raise UsageError("invalid timeout")

    def parseArgs(self, input, output):
        self["input"] = input
        self["output"] = output


if __name__ == "__main__":
    from twisted.application.service import Application
    import sys
    import signal

    application = Application("GstDump")

    try:
        options = Options()
        options.parseOptions()
    except UsageError, e:
        sys.stderr.write("error: %s\n" %e)

        sys.exit(EXIT_ERROR)

    if not options["quiet"]:
        log.startLogging(sys.stdout, setStdout=False)

    dump = DumpService(options["input"], options["output"], options["timeout"],
        muxer=options["muxer"] + "mux")
    dump.setServiceParent(application)

    appService = IService(application)
    reactor.callWhenRunning(appService.startService)
    reactor.addSystemEventTrigger("before", "shutdown", appService.stopService)

    reactor.run()

    sys.exit(dump.exitStatus)
