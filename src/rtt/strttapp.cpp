#include <atomic>
#include <vector>
#include <chrono>
#include <signal.h>

#include "strtt.h"
#include "log.h"
#include "inputparser.h"
#include "consoleinput.h"

#define NATS

#ifdef NATS
#include <nats/nats.h>
#endif

// #define SYSVIEW

#ifdef SYSVIEW
#include "sysview.h"
#endif

#ifdef __linux__
#include <sys/resource.h>
#endif


// CONST //////////////////////////////////////////////////

const int SYSVIEW_COMM_SERVER_PORT = 19111; // the port users will be connecting to

// GLOBAL VARIABLES ///////////////////////////////////////

std::atomic_bool stopApp;

// DEFINES ////////////////////////////////////////////////

#define START_TS auto __start_ts = std::chrono::high_resolution_clock::now()
#define STOP_TS _duration = std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now() - __start_ts).count()

//
//
//
void signalHandler(int signum)
{
    LOG_INFO("Interrupt signal %d received", signum);

    // cleanup and close up stuff here
    // terminate program
    stopApp = true;
}

// INFO:
// https://stackoverflow.com/questions/12207684/how-do-i-terminate-a-thread-in-c11
// https://www.bo-yang.net/2017/11/19/cpp-kill-detached-thread

#ifdef NATS

void avro_write_zigged(std::vector<uint8_t> *buf, uint32_t v) {
    if (v < 0x80) {
        buf->push_back((uint8_t) v);
    } else {
        buf->push_back((uint8_t) ((v & 0x7f) | 0x80));
        avro_write_zigged(buf, v >> 7);
    }
}

void avro_write_int(std::vector<uint8_t> *buf, int v) {
    if (v >= 0) {
        avro_write_zigged(buf, v << 1);
    } else {
        avro_write_zigged(buf, ((-v) << 1) - 1);
    }
}

enum yenc_status { ok, unexpected_byte, unexpected_escape };

typedef struct {
    enum yenc_status status;
    int decoded_bytes;
    int read_bytes;
    uint8_t byte;
} yenc_result;

void yenc_decode(yenc_result *r, std::vector<uint8_t> *target, const std::vector<uint8_t> *source, int offset) {
  int limit = source->size() - offset;
  r->decoded_bytes = 0;
  for(r->read_bytes = 0; r->read_bytes < limit; r->read_bytes++) {
    int b;
    r->byte = (*source)[offset + r->read_bytes];
    switch(r->byte) {
      case 10:
        r->status = ok;
        r->read_bytes++;
        return;
      case 0:
      case 13:
        r->status = unexpected_byte;
        return;
      case 61:
        r->read_bytes++;
        r->byte = (*source)[offset + r->read_bytes];
        b = (r->byte + 256 - 64) % 256;
        switch(b) {
          case 0: case 10: case 13: case 61:
            break;
          default:
            r->status = unexpected_escape;
            return;
        }
        break;
      default:
        b = r->byte;
    }
    target->push_back((b + 256 - 42) % 256);
    r->decoded_bytes++;
  }
  r->status = ok;
  return;
}

#endif

int main(int argc, char **argv)
{

#ifdef __linux__
    // Opportunistic call to renice us, so we can keep up under
    // higher load conditions. This may fail when run as non-root.
    setpriority(PRIO_PROCESS, 0, -11);
#endif

    InputParser input(argc, argv);

    signal(SIGINT, signalHandler);

    log_init();
    debug_level = LOG_LVL_SILENT;

    if (input.cmdOptionExists("-v"))
    {
        debug_level = std::stoi(input.getCmdOption("-v"));
    }

    int _ramKB = 16;
    if (input.cmdOptionExists("-ramsize"))
    {
        std::string opt = input.getCmdOption("-ramsize");
        if (opt.size() > 1 && opt[0] == '0' && (opt[1] == 'x' || opt[1] == 'X'))
        {
            _ramKB = std::stoi(opt, nullptr, 16);
        }
        else
        {
            _ramKB = std::stoi(opt, nullptr, 0);
        }
    }

    int port = SYSVIEW_COMM_SERVER_PORT;
    if (input.cmdOptionExists("-port"))
    {
        port = std::stoi(input.getCmdOption("-port"));
    }

    uint32_t _ramStart = RAM_START;
    if (input.cmdOptionExists("-ramstart"))
    {
        // get ram start from options, value maybe hex or dec
        std::string opt = input.getCmdOption("-ramstart");
        if (opt.size() > 1 && opt[0] == '0' && (opt[1] == 'x' || opt[1] == 'X'))
        {
            _ramStart = std::stoi(opt, nullptr, 16);
        }
        else
        {
            _ramStart = std::stoi(opt, nullptr, 0);
        }
    }

    bool showCycleTime = false;
    if (input.cmdOptionExists("-t"))
    {
        showCycleTime = true;
    }

    bool useTCP = false;
    if (input.cmdOptionExists("-tcp"))
    {
        useTCP = true;
    }

    StRtt *strtt = new StRtt(_ramStart);

    // open stLink
    int res = strtt->open(useTCP);
    if (res != ERROR_OK)
    {
        LOG_ERROR("failed to open STLINK (%d)", res);
        exit(-1);
    }

    // get idCode
    // uint32_t idCode;
    // res = strtt->getIdCode(&idCode);

    // find rtt
    res = strtt->findRtt(_ramKB);
    if (res != ERROR_OK)
    {
        LOG_ERROR("failed to find RTT (%d)", res);
        exit(-1);
    }

    // get channels description
    strtt->getRttDesc();

    // get buff size
    uint32_t sizeR, sizeW;
    res = strtt->getRttBuffSize(0, &sizeR, &sizeW);

#ifdef SYSVIEW
    SysView *_sv;
    _sv = new SysView(port);
#endif

#ifdef NATS
    natsConnection      *nc  = NULL;
    natsSubscription    *sub = NULL;
    natsMsg             *msg = NULL;
    std::vector<uint8_t> nats_buf(2000);
    std::vector<uint8_t> nats_rest(2000);
    yenc_result yenc = { ok, 0, 0, 0 };

    // Connects to the default NATS Server running locally
    natsStatus natsStatus = natsConnection_ConnectTo(&nc, NATS_DEFAULT_URL);
    if(natsStatus == NATS_OK) {
        natsConnection_SubscribeSync(&sub, nc, "strtt_console_down");
    }
#endif

    strtt->addChannelHandler([&](const int index, const std::vector<uint8_t> *buffer)
                             {
                                 if (index == 0)
                                 {
                                     // TERMINAL, print to console
                                     for (uint8_t ch : *buffer)
                                     {
                                         fputc(ch, stdout);
                                     }
                                     fflush(stdout);
#ifdef NATS
                                     natsConnection_Publish(nc, "strtt_console_up", (const void*) buffer->data(), (int)buffer->size());
#endif
                                 }

#ifdef NATS
                                 else if (index == 2 && yenc.status == ok)
                                 {
                                    int records = 0;
                                    int last_end = 0;
                                     for(int i = 0; i < buffer->size(); i++) {
                                        if((*buffer)[i] == '\n') {
                                          records++;
                                          last_end = i;
                                        }
                                     }
                                     LOG_DEBUG("-- starting to write %d messages (%d)", records, last_end);
                                     nats_buf.clear();
                                     // Mark the start of the array of records
                                     avro_write_int(&nats_buf, 1);
                                     yenc_decode(&yenc, &nats_buf, &nats_rest, 0); 
                                     if(yenc.status == ok) {
                                       LOG_DEBUG("--- (old) wrote %d bytes, read %d from %d", yenc.decoded_bytes, yenc.read_bytes, 0);
                                     } else {
                                       LOG_ERROR("--- yenc problem %d", yenc.status);
                                     }
                                     nats_rest.clear();
                                     for(int i = 0; i <= last_end;) {
                                       if(i > 0) avro_write_int(&nats_buf, 1);
                                       yenc_decode(&yenc, &nats_buf, buffer, i);
                                       if (yenc.status == ok) {
                                         LOG_DEBUG("--- wrote %d bytes, read %d from %d", yenc.decoded_bytes, yenc.read_bytes, i);
                                         if (yenc.read_bytes == 0) {
                                           LOG_ERROR("yenc decode did not find any data");
                                           break;
                                         }
                                         i += yenc.read_bytes;
                                       } else {
                                         LOG_ERROR("--- yenc problem %d", yenc.status);
                                         break;
                                       }
                                     }
                                     // Mark the end of the array of records
                                     nats_buf.push_back(0);
                                     natsConnection_Publish(nc, "strtt_up", (const void*) nats_buf.data(), (int) nats_buf.size());
                                     // Keep the rest of the buffer for next time
                                     for(int i = last_end + 1; i < buffer->size(); i++) {
                                       nats_rest.push_back((*buffer)[i]);
                                     }
                                 }
#endif
#ifdef SYSVIEW
                                 else if (index == 1)
                                 {
                                     LOG_OUTPUT("SysView size: %d ", (int)buffer->size());
                                     _sv->saveFromSTM(buffer);
                                 }
#endif
                             });

    ConsoleInput console;
    std::vector<uint8_t> str;
    double _duration;
    while (!stopApp)
    {
        START_TS;

        // read rtt
        res = strtt->readRtt();

        if (res != ERROR_OK)
        {
            LOG_ERROR("readRtt returned error %d, program is exiting", res);
            stopApp = true;
        }

        // read console
        while (console.isChar())
        {
            uint8_t ch = console.getChar();
            str.push_back(ch);
        }

        // write rtt
        if (str.size() > 0)
        {
            strtt->writeRtt(0, &str);
        }

#ifdef NATS
        if (NATS_OK == natsSubscription_NextMsg(&msg, sub, 0)) {
            std::vector<uint8_t> buffer;
            int l = natsMsg_GetDataLength(msg);
            const char* data = natsMsg_GetData(msg);
            for(int i = 0; i < l; i++) {
              buffer.push_back(data[i]);
            }
            strtt->writeRtt(0, &buffer);
            // Don't forget to destroy the message!
            natsMsg_Destroy(msg);
        }
#endif
#ifdef SYSVIEW
        // write SysView
        if (_sv->dataToSTM())
        {
            auto data = _sv->getDataToSTM();
            strtt->writeRtt(1, &data);
        }
#endif

        if (showCycleTime)
        {
            STOP_TS;
            LOG_USER("Cycle time: %dms", (int)_duration);
        }
    }

    strtt->close();
#ifdef NATS
    natsConnection_Destroy(nc);
#endif
    return 0;
}
