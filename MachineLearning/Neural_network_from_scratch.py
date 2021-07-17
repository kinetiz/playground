# Work left
# - why gradient don't calculate activation part ? -> why input*error not delta(sig)*error

from numpy import exp, array, random, dot


class NeuralNetwork():
    ## Define class structure ##################
    # 1. constructor(num_neurons, num_layer)
    # - model = set of weights
    # 2. train(input, output) : train model
    # 3. predict(new_input) return result
    ####################
    _Sigmoid = 'sigmoid'
    _Relu = 'relu'
    _Tanh = 'tanh'

    def __init__(self, learning_rate=0.05, activation=_Sigmoid):
        # set seed for random function to produce the same result every run
        random.seed(1)
        # random initial weights [-1,1)
        self.learning_rate = learning_rate
        self.weights = 2 * random.random((3, 1)) - 1
        self.activation = activation

    # Train networks
    def train(self, X, Y, round=10):
        ### Explain Algorithm ############################################################
        # Update weight until convergence or reach the defined round.
        # W_new = W - Delta(CostFunction)*learningRate
        # CostFunction  = (predicted - actual)^2
        #               = (sig(Input*Weight) - actual)^2
        # ** Input=X, actual=Y, Weight=W
        # Delta(CostFunction) = Derivative of CostFunction respect to weight
        #                   => apply chain rule from calculus, we diff in&out then gets:-
        # Delta(Cost) = 2(sig(X*W) - Y) * delta(sig(X*W));
        # ** 2 can be omitted as we'r gonna multiply with learning rate anyway.
        # Thus, W_new  = W - lr*(sig(X*W) - Y)*delta(sig(X*W))
        #              = W - lr*Error*delta(sig(X*W))
        ###############################################################
        for i in range(round):
            print("training: " + str(i))
            # W = W - lr*(act(X*W) - Y)*delta(act(X*W))
            print(X)
            print(self.weights)
            xw = dot(X, self.weights)
            print(xw)
            act = self.activate(xw)
            error = act - Y
            delta_act = self.delta_activate(xw)
            delta_cost = error * delta_act
            # update weight
            self.weights = self.weights - self.learning_rate*delta_cost
        return 1

    # Predict data - throw input through the network
    # [ input->hidden_weight->hidden_activated->output ]
    # input: [x1,x2,x3] or
    # return: y
    def predict(self, input):
        # Calculate result for layer-1 before passing activation function
        pre_activated_result = dot(input, self.weights)
        # Pass activation function
        output = self.activate(pre_activated_result)
        return output

    # Activation function
    def activate(self, x):
        result = 0
        # use activation function according to the configuration
        if self.activation == self._Sigmoid:
            result = 1 / (1 + exp(-x))
        else:
            result = 1 / (1 + exp(-x))
        return result

    # Derivative of the activation function
    def delta_activate(self, x):
        delta = 0
        if self.activation == self._Sigmoid:
            delta = x * (1 - x)
        else:
            delta = x * (1 - x)

        return delta


if __name__ == "__main__":
    # initialise nnw object
    nnw = NeuralNetwork()

    print("Inspect initial random weights")
    print(nnw.weights)

    # Set features and target output
    featuresInput = array([[0, 0, 1], [1, 1, 1], [1, 0, 1], [0, 1, 1]])
    targetOutput = array([[0, 1, 1, 0]]).T

    # Train the network
    nnw.train(featuresInput, targetOutput, round=10000)

    # See trained weights
    print("Trained weights:")
    print(nnw.weights)

    # Predict unseen data
    unseenInput = array([[1, 1, 0]])
    predictedOutput = nnw.predict(unseenInput)
    print("Predicted output from input " + str(unseenInput) + ": ")
    print(predictedOutput)