import sys
import cv2 as cv
import numpy as np
import onnxruntime

class Inference_worker():
    def __init__(self, task_name):
        self.session = onnxruntime.InferenceSession(f'{task_name}')
    
    def img2tensor(self, x):
        x = cv.resize(x, (224, 224))
                
        x = cv.cvtColor(x, cv.COLOR_BGR2RGB)
        x = x.transpose((2, 0, 1))
        
        x = self.imagenet_normalize(x)

        x = np.expand_dims(x, axis=0)
        x = x.astype(np.float32)

        return x

    def imagenet_normalize(self, x):
        mean_vec = np.array([0.485, 0.456, 0.406])
        stddev_vec = np.array([0.229, 0.224, 0.225])
        norm_img_data = np.zeros(x.shape).astype('float32')
        for i in range(x.shape[0]):
            norm_img_data[i, :, :] = (
                x[i, :, :]/255 - mean_vec[i]) / stddev_vec[i]
        return norm_img_data


    def softmax(self, x):
        c = np.max(x)
        exp_a = np.exp(x-c)
        sum_exp_a = np.sum(exp_a)
        return exp_a / sum_exp_a


    def preprocess(self, x):
        return self.img2tensor(x)

    def inference(self, x):
        return self.session.run(None, {'input': x})[0]

    def postprocess(self, x):
        return self.softmax(x)



if __name__ == '__main__':
    run_process = sys.argv[1]
    image_name = sys.argv[2]

    tester = Inference_worker(run_process)
    x = cv.imread(f'{image_name}')
    x = tester.preprocess(x)
    x = tester.inference(x)
    x = tester.postprocess(x)

    accuracy = np.max(x)
    class_name = np.argmax(x)

    with open('imagenet_classes.txt', 'r') as f:
        categories = [s.strip() for s in f.readlines()]

    print(f'object^{categories[class_name]}^accuracy^{accuracy*100}%')
    
    