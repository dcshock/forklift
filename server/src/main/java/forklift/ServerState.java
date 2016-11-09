package forklift;

/**
 * The various states which a server may be in
 *
 * Created by afrieze on 11/4/16.
 */
public enum ServerState {
    LATENT,
    STARTING,
    ERROR,
    RUNNING,
    STOPPING,
    STOPPED
}
