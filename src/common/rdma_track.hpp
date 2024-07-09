
enum RdmaAction {
    RDMA_R,
    RDMA_W
};

struct RdmaTrackingMessage {
    std::string mMessage;
    std::string mDestination;
    std::string mSource;
    uint64_t mValue;
    bool bSrcDest;

public:
    RdmaTrackingMessage(std::string message, std::string destination, uint64_t value) {
        mMessage = message;
        mDestination = destination;
        mValue = value;
        bSrcDest = true;
    }

    RdmaTrackingMessage(std::string message, std::string source) {
        mMessage = message;
        mSource = source;
        bSrcDest = false;
    }
};

struct RdmaTracker {
    std::vector<RdmaTrackingMessage> mMessages;

public:
    void addWriteMessage(std::string msg, std::string destination, uint64_t value) {
        RdmaTrackingMessage rtm(msg, destination, value);
        mMessages.push_back(rtm);
    }

    void addReadMessage(std::string msg, std::string source) {
        RdmaTrackingMessage rtm(msg, source);
        mMessages.push_back(rtm);
    }

    void clear() { mMessages.clear();}
    void list() {
        for (RdmaTrackingMessage m : mMessages) {
            if (m.bSrcDest == true)
                std::cout << m.mMessage << "W: " << m.mDestination << " = " << m.mValue << std::endl;
            else
                std::cout << m.mMessage << "R: " << m.mSource << std::endl;
        }
    }
};

RdmaTracker RDMA_tracker;
#define W(msg, dest, source) {RDMA_tracker.addWriteMessage(msg, dest, source); dest = (source);}