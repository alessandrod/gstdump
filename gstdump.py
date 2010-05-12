#!/usr/bin/env python
#
# This program is free software. It comes without any warranty, to
# the extent permitted by applicable law. You can redistribute it
# and/or modify it under the terms of the Do What The Fuck You Want
# To Public License, Version 2, as published by Sam Hocevar. See
# http://sam.zoy.org/wtfpl/COPYING for more details.
#
# Author: Alessandro Decina <alessandro.d@gmail.com>

import os

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
from twisted.python.usage import Options as BaseOptions, UsageError

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_LINK_TIMEOUT = 2
EXIT_EOS_TIMEOUT = 3

# time in seconds
DEFAULT_LINK_TIMEOUT = 30
DEFAULT_EOS_TIMEOUT = 5

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

class H264Parser(gst.Element):
    __gstdetails__ = ("h264Parser", "Filter/Video",
            "Blah", "Alessandro Decina")

    sinktemplate = gst.PadTemplate ("sink",
            gst.PAD_SINK, gst.PAD_ALWAYS, gst.Caps("video/x-h264"))
    srctemplate = gst.PadTemplate ("src",
            gst.PAD_SRC, gst.PAD_ALWAYS, gst.Caps("video/x-h264"))

    def __init__(self):
        gst.Element.__init__(self)

        self.sinkpad = gst.Pad(self.sinktemplate)
        self.sinkpad.set_setcaps_function(self.sink_setcaps)
        self.sinkpad.set_chain_function(self.chain)
        self.add_pad(self.sinkpad)

        self.srcpad = gst.Pad(self.srctemplate)
        self.add_pad(self.srcpad)

        self.buffers = []

    def sink_setcaps(self, pad, caps):
        return self.srcpad.set_caps(caps)

    def chain(self, pad, buf):
        caps = pad.props.caps
        if caps is None or not caps[0].has_key("width"):
            logInfo("%s incomplete caps, buffering %s" % (self, caps))
            self.buffers.append(buf)
            return gst.FLOW_OK

        ret = gst.FLOW_OK
        if self.buffers:
            logInfo("%s caps complete, pushing out buffered buffers" % self)

            while ret == gst.FLOW_OK:
                try:
                    pbuf = self.buffers.pop(0)
                except IndexError:
                    break
                pbuf.set_caps(self.srcpad.props.caps)
                ret = self.srcpad.push(pbuf)

            self.buffers = []

        if ret != gst.FLOW_OK:
            return ret

        buf.set_caps(self.srcpad.props.caps)
        res = self.srcpad.push(buf)
        return res

gobject.type_register(H264Parser)
gst.element_register(H264Parser, "twih264parse", gst.RANK_MARGINAL)

class H264Codec(VideoCodec):
    def createBinReal(self):
        codecBin = gst.Bin()

        self.h264parse = h264parse = gst.element_factory_make("h264parse")
        self.h264parse.props.output_format = 0
        twih264parse = H264Parser()
        fixTimestamps = FixTimestamps()
        codecBin.add(h264parse, twih264parse, fixTimestamps)

        h264parse.link(twih264parse)
        twih264parse.link(fixTimestamps)

        sinkpad = gst.GhostPad("sink", h264parse.get_pad("sink"))
        srcpad = gst.GhostPad("src", fixTimestamps.get_pad("src"))

        # workaround a bug in h264parse, fixed by -bad 52f5f4
        pad = h264parse.get_pad("src")
        pad.connect("notify::caps", self.checkCaps)

        codecBin.add_pad(sinkpad)
        codecBin.add_pad(srcpad)

        return codecBin

    def checkCaps(self, pad, pspec):
        if pad.props.caps is None or pad.props.caps[0].has_key("width"):
            return

        logInfo("pad %s setting caps again" % pad)
        # set the caps again so they get updated again and width/height are
        # filled with correct values
        caps = pad.props.caps
        caps = gst.Caps(caps)
        caps[0]["width"] = 42
        caps[0]["height"] = 42
        self.h264parse.get_pad("sink").set_caps(caps)

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

    def chain(self, pad, buf):
        ts = buf.timestamp
        if self.log_ts == gst.CLOCK_TIME_NONE or ts >= self.log_ts:
            self.log_ts = ts + self.log_interval
            logInfo("now at %s" % gst.TIME_ARGS(ts))

        return self.srcpad.push(buf)

gobject.type_register(LogTimestamps)
gst.element_register(LogTimestamps, "logtimestamps", gst.RANK_MARGINAL)


class FixTimestamps(gst.Element):
    __gstdetails__ = ("FixTimestamps", "Filter",
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
        self.sinkpad.set_chain_function(self.chain)
        self.add_pad(self.sinkpad)

        self.srcpad = gst.Pad(self.srctemplate)
        self.add_pad(self.srcpad)

        self.reset()

    def reset(self):
        self.segment = gst.Segment()
        self.prev_buf = None
        self.expected_ts = gst.CLOCK_TIME_NONE

    def sink_event(self, pad, event):
        if event.type == gst.EVENT_NEWSEGMENT:
            tup = event.parse_new_segment_full()
            update, rate, applied_rate, fmt, start, stop, time = tup
            self.segment.set_newsegment_full(update, rate, applied_rate, fmt,
                    start, stop, time)

            self.expected_ts = start

        elif event.type == gst.EVENT_FLUSH_STOP:
            self.reset()

        return self.sinkpad.event_default(event)

    def sink_setcaps(self, pad, caps):
        return self.srcpad.set_caps(caps)

    def chain(self, pad, buf):
        if self.expected_ts == gst.CLOCK_TIME_NONE:
            logError("no expected ts... no NEWSEGMENT")

            return gst.FLOW_UNEXPECTED

        gst_time = gst.TIME_ARGS

        if self.prev_buf is None:
            self.prev_buf = buf

            return gst.FLOW_OK

        prev_buf = self.prev_buf
        self.prev_buf = buf

        if prev_buf.timestamp == gst.CLOCK_TIME_NONE:
            prev_buf.timestamp = self.expected_ts

        elif prev_buf.timestamp != self.expected_ts:
            if prev_buf.timestamp > self.expected_ts:
                diff = prev_buf.timestamp - self.expected_ts
            else:
                diff = self.expected_ts - prev_buf.timestamp

            if diff > buf.timestamp or diff > 1 * gst.SECOND:
                logInfo("expected %s got %s" % (gst_time(prev_buf.timestamp),
                        gst_time(self.expected_ts)))

                self.prev_buf = buf
                self.expected_ts = self.segment.start

                return gst.FLOW_OK

        if prev_buf.duration == gst.CLOCK_TIME_NONE \
                and buf.timestamp != gst.CLOCK_TIME_NONE \
                and buf.timestamp > prev_buf.timestamp:
            duration = buf.timestamp - prev_buf.timestamp
            prev_buf.duration = duration

        self.expected_ts = prev_buf.timestamp + prev_buf.duration

        prev_buf.set_caps(self.srcpad.props.caps)
        return self.srcpad.push(prev_buf)

gobject.type_register(FixTimestamps)
gst.element_register(FixTimestamps, "fixtimestamps", gst.RANK_MARGINAL)

class RawIntAudioCodec(AudioCodec):
    def createBinReal(self):
        codecBin = gst.Bin()
        encoder = gst.element_factory_make("faac")
        fixTimestamps = FixTimestamps()

        codecBin.add(encoder, fixTimestamps)
        encoder.link(fixTimestamps)

        sinkpad = gst.GhostPad("sink", encoder.get_pad("sink"))
        srcpad = gst.GhostPad("src", fixTimestamps.get_pad("src"))

        codecBin.add_pad(sinkpad)
        codecBin.add_pad(srcpad)

        return codecBin


SUPPORTED_CODECS = [
        H264Codec("video/x-h264"),
        VideoCodec("video/mpeg, mpegversion=(int)4", "mpeg4videoparse"),
        VideoCodec("video/mpeg, mpegversion={1, 2}", "mpegvideoparse"),
        VideoCodec("image/jpeg"),
        VideoCodec("video/x-raw-rgb", "ffmpegcolorspace ! x264enc"),
        VideoCodec("video/x-raw-yuv", "ffmpegcolorspace ! x264enc"),
        #AudioCodec("audio/mpeg, mpegversion=(int){2, 4}", "aacparse"),
        AudioCodec("audio/mpeg, mpegversion=(int)1, layer=[1, 3]", "mp3parse"),
        #AudioCodec("audio/x-raw-int", "faac"),
        RawIntAudioCodec("audio/x-raw-int"),
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

class UnParser(gst.Element):
    __gstdetails__ = ("UnParser", "Parser",
            'I unparse parsed stuff (for real)', 'Alessandro Decina')

    sinktemplate = gst.PadTemplate ("sink",
            gst.PAD_SINK, gst.PAD_ALWAYS, gst.caps_new_any())
    srctemplate = gst.PadTemplate ("src",
            gst.PAD_SRC, gst.PAD_ALWAYS, gst.caps_new_any())

    def __init__(self):
        gst.Element.__init__(self)

        self.sinkpad = gst.Pad(self.sinktemplate)
        self.sinkpad.set_setcaps_function(self.sink_setcaps)
        self.sinkpad.set_chain_function(self.chain)
        self.add_pad(self.sinkpad)

        self.srcpad = gst.Pad(self.srctemplate)
        self.add_pad(self.srcpad)

    def sink_setcaps(self, pad, caps):
        caps = gst.Caps(caps)
        for structure in caps:
            if structure.has_key("parsed"):
                structure["parsed"] = False
            if structure.has_key("framed"):
                structure["framed"] = False

        return self.srcpad.set_caps(caps)

    def chain(self, pad, buf):
        #print self, "pushing", buf.size
        buf.set_caps(self.srcpad.props.caps)
        res = self.srcpad.push(buf)
        return res


gobject.type_register(UnParser)
gst.element_register(UnParser, "unparser", gst.RANK_MARGINAL)

class TimeoutCall(object):
    call = None

    def __init__(self, timeout, callback, *args, **kw):
        self.timeout = timeout
        self.callback = callback
        self.args = args
        self.kw = kw

    def start(self):
        assert self.call is None
        self.call = self.callLater(self.timeout, self.callback,
                *self.args, **self.kw)

    def cancel(self):
        if self.call is None:
            return

        self.call.cancel()
        self.call = None

    def callLater(self, timeout, callback, *args, **kw):
        return reactor.callLater(timeout, callback, *args, **kw)


class DumpService(Service):
    def __init__(self, uri, outputFilename, linkTimeout=DEFAULT_LINK_TIMEOUT,
            eosTimeout=DEFAULT_EOS_TIMEOUT):
        self.uri = uri
        self.outputFilename = outputFilename
        self.pipeline = None
        self.exitStatus = None
        self.linkTimeout = TimeoutCall(linkTimeout, self.linkTimeoutCb)
        self.eosTimeout = TimeoutCall(eosTimeout, self.eosTimeoutCb)
        self.blockedPads = []

    def startService(self):
        if self.pipeline is not None:
            raise DumpError("dump already started")

        self.buildPipeline()

        self.linkTimeout.start()
        self.pipeline.set_state(gst.STATE_PLAYING)

        Service.startService(self)

    def stopService(self, stopReactor=False):
        if self.pipeline is not None:
            # shutdown() calls reactor.stop() which in turn calls
            # stopService(). So we need to call shutdown only if shutdown hasn't
            # been called yet.
            self.shutdown(EXIT_OK, stopReactor)
        Service.stopService(self)

    def busEosCb(self, bus, message):
        logInfo("gstreamer eos")
        self.eosTimeout.cancel()
        fast_start = self.muxer.props.faststart_file
        os.unlink(fast_start)
        self.shutdown(EXIT_OK)

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

        self.linkTimeout.cancel()

        logInfo("found %s codec %s" % (codec.codecType, codec.caps.to_string()[:500]))

        queue = gst.element_factory_make("queue")
        queue.props.max_size_bytes = 0
        queue.props.max_size_time = 0
        queue.props.max_size_buffers = 0
        unparser = UnParser()
        codecBin = codec.createBin()
        self.pipeline.add(unparser, codecBin, queue)
        unparser.set_state(gst.STATE_PLAYING)
        codecBin.set_state(gst.STATE_PLAYING)
        queue.set_state(gst.STATE_PLAYING)

        try:
            pad.link(unparser.get_pad("sink"))
            gst.element_link_many(unparser, codecBin, queue)
        except gst.LinkError, e:
            logError("couldn't link unparser codecBin and queue %s" % e)
            self.callLater(0, self.shutdown, EXIT_ERROR)

            return

        queuePad = queue.get_pad("src")
        if codec.codecType == 'audio':
            name = "audio_%d"
        else:
            name = "video_%d"
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

        self.muxer = gst.element_factory_make("qtmux")
        self.muxer.props.faststart = True
        self.muxer.set_locked_state(True)
        self.filesink = gst.element_factory_make("filesink")
        self.filesink.props.location = self.outputFilename

        self.pipeline.add(self.uridecodebin, self.muxer, self.filesink)
        gst.element_link_many(self.muxer, self.filesink)

    def cleanPipeline(self):
        self.pipeline.set_state(gst.STATE_NULL)
        self.pipeline = None

    def shutdown(self, exitStatus, stopReactor=True):
        exit = dict((v, k) for k, v in globals().iteritems()
                if k.startswith("EXIT_"))[exitStatus]
        logInfo("shutdown %s" % exit)
        self.exitStatus = exitStatus

        self.cleanPipeline()
        if stopReactor:
            reactor.stop()

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
        logInfo("forcing EOS")
        self.eosTimeout.start()
        self.doControlledShutdown()

    def doControlledShutdown(self):
        logInfo("doing regular controlled shutdown")
        #self.pipeline.send_event(gst.event_new_eos())
        for pad in self.muxer.sink_pads():
            pad.send_event(gst.event_new_eos())

    def eosTimeoutCb(self):
        self.shutdown(EXIT_EOS_TIMEOUT)


class Options(BaseOptions):
    synopsis = ""
    optParameters = [
            ["timeout", "t", DEFAULT_LINK_TIMEOUT, "timeout"],
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

    dump = DumpService(options["input"], options["output"], options["timeout"])
    dump.setServiceParent(application)

    appService = IService(application)
    reactor.callWhenRunning(appService.startService)
    reactor.addSystemEventTrigger("before", "shutdown", appService.stopService)

    # add SIGINT handler before calling reactor.run() which would otherwise
    # install twisted's SIGINT handler
    signal.signal(signal.SIGINT, dump.sigInt)
    signal.signal(signal.SIGUSR2, dump.sigUsr2)

    reactor.run()

    sys.exit(dump.exitStatus)
